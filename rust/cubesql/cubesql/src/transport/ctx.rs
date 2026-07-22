use datafusion::{arrow::datatypes::DataType, logical_plan::Column};
use std::{
    collections::{BTreeSet, HashMap},
    ops::RangeFrom,
    sync::Arc,
};
use uuid::Uuid;

use crate::{sql::ColumnType, transport::SqlGenerator};

use super::{CubeMeta, CubeMetaDimension, CubeMetaMeasure, V1CubeMetaExt};

pub const CATALOG_PUBLIC_SCHEMA_NAME: &str = "public";
pub const CATALOG_PUBLIC_SCHEMA_OID: u32 = 2200;
const CATALOG_DYNAMIC_SCHEMA_OID_START: u32 = 18000;

#[derive(Debug)]
pub struct MetaContext {
    pub cubes: Vec<CubeMeta>,
    /// SQL-visible relations. Each record is a catalog identity for one schema projection of a
    /// semantic Cube model. Today every model has one implicit `public` projection; later
    /// declarations may add more records without changing catalog consumers.
    pub catalog_projections: Vec<CatalogProjection>,
    pub member_to_data_source: HashMap<String, String>,
    pub data_source_to_sql_generator: HashMap<String, Arc<dyn SqlGenerator + Send + Sync>>,
    pub compiler_id: Uuid,
    /// DateTime when MetaContext was created, but it can be used as last schema update when
    /// CompilerCache is used
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub struct CatalogProjection {
    pub oid: u32,
    pub record_oid: u32,
    pub array_handler_oid: u32,
    pub schema: String,
    pub schema_oid: u32,
    pub name: String,
    pub description: Option<String>,
    pub columns: Vec<CubeMetaColumn>,
}

#[derive(Debug, Clone)]
pub struct CubeMetaColumn {
    pub oid: u32,
    pub name: String,
    pub description: Option<String>,
    pub column_type: ColumnType,
    pub can_be_null: bool,
}

#[derive(Clone, Debug)]
pub enum DataSource<'meta> {
    Unrestricted,
    Specific(&'meta str),
}

#[derive(Debug, thiserror::Error)]
pub enum DataSourceError {
    #[error("Multiple data sources found, '{0}' and '{1}'")]
    Conflict(String, String),
    #[error("Data source not found for member '{0}'")]
    Missing(String),
}

impl<'meta> DataSource<'meta> {
    pub fn specific_or<E>(self, err: E) -> Result<&'meta str, E> {
        match self {
            DataSource::Unrestricted => Err(err),
            DataSource::Specific(data_source) => Ok(data_source),
        }
    }

    pub fn merge(&self, other: &Self) -> Result<Self, DataSourceError> {
        match (self, other) {
            (Self::Unrestricted, ds) | (ds, Self::Unrestricted) => Ok(ds.clone()),
            (Self::Specific(s1), Self::Specific(s2)) => {
                if s1 == s2 {
                    Ok(self.clone())
                } else {
                    Err(DataSourceError::Conflict(s1.to_string(), s2.to_string()))
                }
            }
        }
    }
}

impl MetaContext {
    pub fn new(
        cubes: Vec<CubeMeta>,
        member_to_data_source: HashMap<String, String>,
        data_source_to_sql_generator: HashMap<String, Arc<dyn SqlGenerator + Send + Sync>>,
        compiler_id: Uuid,
    ) -> Self {
        let custom_schemas: BTreeSet<String> = cubes
            .iter()
            .flat_map(|cube| cube.sql_schemas.iter().flatten())
            .filter(|schema| schema.as_str() != CATALOG_PUBLIC_SCHEMA_NAME)
            .cloned()
            .collect();
        let mut schema_oids = HashMap::from([(
            CATALOG_PUBLIC_SCHEMA_NAME.to_string(),
            CATALOG_PUBLIC_SCHEMA_OID,
        )]);
        let mut next_schema_oid = CATALOG_DYNAMIC_SCHEMA_OID_START;
        for schema in custom_schemas {
            schema_oids.insert(schema, next_schema_oid);
            next_schema_oid += 1;
        }

        // 18000 is above the system table OID range. Reserve dynamic namespace OIDs first so
        // relation, record, array, and column OIDs cannot collide with them.
        let mut projection_sources: Vec<(&CubeMeta, String)> = cubes
            .iter()
            .flat_map(|cube| {
                let schemas = cube
                    .sql_schemas
                    .clone()
                    .unwrap_or_else(|| vec![CATALOG_PUBLIC_SCHEMA_NAME.to_string()]);
                schemas.into_iter().map(move |schema| (cube, schema))
            })
            .collect();
        // Preserve the legacy public-only ordering exactly. Explicit projections, however, must
        // have stable OIDs for the same visible metadata set even if the transport orders models
        // differently.
        if cubes.iter().any(|cube| cube.sql_schemas.is_some()) {
            projection_sources.sort_by(|(left_cube, left_schema), (right_cube, right_schema)| {
                (left_schema, &left_cube.name).cmp(&(right_schema, &right_cube.name))
            });
        }

        let mut oid_iter: RangeFrom<u32> = next_schema_oid..;
        let catalog_projections: Vec<CatalogProjection> = projection_sources
            .into_iter()
            .map(|(cube, schema)| CatalogProjection {
                oid: oid_iter.next().unwrap_or(0),
                record_oid: oid_iter.next().unwrap_or(0),
                array_handler_oid: oid_iter.next().unwrap_or(0),
                schema_oid: schema_oids[&schema],
                schema,
                name: cube.name.clone(),
                description: cube.description.clone(),
                columns: cube
                    .get_columns()
                    .iter()
                    .map(|column| CubeMetaColumn {
                        oid: oid_iter.next().unwrap_or(0),
                        name: column.get_name().clone(),
                        description: column.get_description().clone(),
                        column_type: column.get_column_type().clone(),
                        can_be_null: column.sql_can_be_null(),
                    })
                    .collect(),
            })
            .collect();

        Self {
            cubes,
            catalog_projections,
            member_to_data_source,
            data_source_to_sql_generator,
            compiler_id,
            created_at: chrono::Utc::now(),
        }
    }

    pub fn data_source_for_member_name(
        &self,
        member: &str,
    ) -> Result<DataSource<'_>, DataSourceError> {
        if self.is_synthetic_field(member) {
            return Ok(DataSource::Unrestricted);
        }

        match self.member_to_data_source.get(member) {
            Some(data_source) => Ok(DataSource::Specific(data_source.as_ref())),
            None => Err(DataSourceError::Missing(member.to_string())),
        }
    }

    pub fn data_source_for_member_names<'mem>(
        &self,
        members: impl IntoIterator<Item = &'mem str>,
    ) -> Result<DataSource<'_>, DataSourceError> {
        members
            .into_iter()
            .map(|member| self.data_source_for_member_name(member))
            .try_fold(DataSource::Unrestricted, |l, r| l.merge(&r?))
    }

    pub fn find_cube_with_name(&self, name: &str) -> Option<&CubeMeta> {
        self.cubes.iter().find(|&cube| cube.name == name)
    }

    pub fn find_cube_by_column<'meta, 'alias>(
        &'meta self,
        alias_to_cube: &'alias Vec<(String, String)>,
        column: &Column,
    ) -> Option<(&'alias str, &'meta CubeMeta)> {
        (if let Some(rel) = column.relation.as_ref() {
            alias_to_cube.iter().find(|(a, _)| a == rel)
        } else {
            alias_to_cube.iter().find(|(_, c)| {
                if let Some(cube) = self.find_cube_with_name(c) {
                    // TODO replace cube.contains_member(&cube.member_name(...)) with searching by prepared column names
                    cube.contains_member(&cube.member_name(&column.name))
                } else {
                    false
                }
            })
        })
        .and_then(|(a, c)| self.find_cube_with_name(c).map(|cube| (a.as_str(), cube)))
    }

    pub fn find_cube_by_column_for_replacer<'alias>(
        &self,
        alias_to_cube: &'alias Vec<((String, String), String)>,
        column: &Column,
    ) -> Vec<((&'alias str, &'alias str), &CubeMeta)> {
        if let Some(rel) = column.relation.as_ref() {
            alias_to_cube
                .iter()
                .filter_map(|((old, new), c)| {
                    if old == rel {
                        self.find_cube_with_name(c)
                            .map(|cube| ((old.as_str(), new.as_str()), cube))
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            alias_to_cube
                .iter()
                .filter_map(|((old, new), c)| {
                    if let Some(cube) = self.find_cube_with_name(c) {
                        // TODO replace cube.contains_member(&cube.member_name(...)) with searching by prepared column names
                        if cube.contains_member(&cube.member_name(&column.name)) {
                            return Some(((old.as_str(), new.as_str()), cube));
                        }
                    }

                    None
                })
                .collect()
        }
    }

    pub fn find_measure_with_name(&self, name: &str) -> Option<&CubeMetaMeasure> {
        let mut cube_and_member_name = name.split(".");
        let cube_name = cube_and_member_name.next()?;
        let member_name = cube_and_member_name.next()?;
        let cube = self.find_cube_with_name(cube_name)?;
        cube.lookup_measure(member_name)
    }

    pub fn find_dimension_with_name(&self, name: &str) -> Option<&CubeMetaDimension> {
        let mut cube_and_member_name = name.split(".");
        let cube_name = cube_and_member_name.next()?;
        let member_name = cube_and_member_name.next()?;
        let cube = self.find_cube_with_name(cube_name)?;
        cube.lookup_dimension(member_name)
    }

    pub fn is_synthetic_field(&self, name: &str) -> bool {
        let mut cube_and_member_name = name.split(".");
        let Some(cube_name) = cube_and_member_name.next() else {
            return false;
        };
        let Some(member_name) = cube_and_member_name.next() else {
            return MetaContext::is_synthetic_field_name(cube_name);
        };

        if self.find_cube_with_name(cube_name).is_some() {
            MetaContext::is_synthetic_field_name(member_name)
        } else {
            false
        }
    }

    pub fn is_synthetic_field_name(field_name: &str) -> bool {
        field_name == "__user" || field_name == "__cubeJoinField"
    }

    pub fn find_df_data_type(&self, member_name: &str) -> Option<DataType> {
        let (cube_name, _) = member_name.split_once(".")?;

        self.find_cube_with_name(cube_name)?
            .df_data_type(member_name)
    }

    pub fn find_catalog_projection_with_oid(&self, oid: u32) -> Option<&CatalogProjection> {
        self.catalog_projections
            .iter()
            .find(|projection| projection.oid == oid)
    }

    pub fn find_catalog_projection(&self, schema: &str, name: &str) -> Option<&CatalogProjection> {
        self.catalog_projections.iter().find(|projection| {
            projection.schema.eq_ignore_ascii_case(schema)
                && projection.name.eq_ignore_ascii_case(name)
        })
    }

    pub fn cube_has_join(&self, cube_name: &str, join_name: &str) -> bool {
        if let Some(cube) = self.find_cube_with_name(cube_name) {
            if let Some(joins) = &cube.joins {
                return joins.iter().any(|j| j.name == join_name);
            }
        }

        return false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::CubeMetaType;

    #[test]
    fn test_legacy_models_create_public_catalog_projections() {
        let test_cubes = vec![
            CubeMeta {
                name: "test1".to_string(),
                description: None,
                sql_schemas: None,
                title: None,
                r#type: CubeMetaType::Cube,
                dimensions: vec![],
                measures: vec![],
                segments: vec![],
                joins: None,
                folders: None,
                nested_folders: None,
                hierarchies: None,
                meta: None,
            },
            CubeMeta {
                name: "test2".to_string(),
                description: None,
                sql_schemas: None,
                title: None,
                r#type: CubeMetaType::Cube,
                dimensions: vec![],
                measures: vec![],
                segments: vec![],
                joins: None,
                folders: None,
                nested_folders: None,
                hierarchies: None,
                meta: None,
            },
        ];

        // TODO
        let test_context =
            MetaContext::new(test_cubes, HashMap::new(), HashMap::new(), Uuid::new_v4());

        assert_eq!(2, test_context.catalog_projections.len());
        assert!(test_context.catalog_projections.iter().all(|projection| {
            projection.schema == CATALOG_PUBLIC_SCHEMA_NAME
                && projection.schema_oid == CATALOG_PUBLIC_SCHEMA_OID
        }));

        match test_context.find_catalog_projection_with_oid(18000) {
            Some(table) => assert_eq!(18000, table.oid),
            _ => panic!("wrong oid!"),
        }

        match test_context.find_catalog_projection(CATALOG_PUBLIC_SCHEMA_NAME, "test2") {
            Some(table) => {
                assert_eq!(18005, table.oid);
                assert_eq!(CATALOG_PUBLIC_SCHEMA_OID, table.schema_oid);
            }
            _ => panic!("wrong name!"),
        }
    }

    #[test]
    fn declared_schemas_create_independent_catalog_projections() {
        let cube = CubeMeta {
            name: "date".to_string(),
            description: None,
            sql_schemas: Some(vec!["sales".to_string(), "finance".to_string()]),
            title: None,
            r#type: CubeMetaType::View,
            dimensions: vec![],
            measures: vec![],
            segments: vec![],
            joins: None,
            folders: None,
            nested_folders: None,
            hierarchies: None,
            meta: None,
        };
        let context = MetaContext::new(vec![cube], HashMap::new(), HashMap::new(), Uuid::new_v4());

        let sales = context.find_catalog_projection("sales", "date").unwrap();
        let finance = context.find_catalog_projection("finance", "date").unwrap();
        assert_ne!(sales.oid, finance.oid);
        assert_ne!(sales.record_oid, finance.record_oid);
        assert_ne!(sales.array_handler_oid, finance.array_handler_oid);
        assert_ne!(sales.schema_oid, finance.schema_oid);
        assert_eq!(sales.schema_oid, CATALOG_DYNAMIC_SCHEMA_OID_START + 1);
        assert_eq!(finance.schema_oid, CATALOG_DYNAMIC_SCHEMA_OID_START);
        assert!(context.find_catalog_projection("public", "date").is_none());
    }

    #[test]
    fn explicit_projection_oids_do_not_depend_on_metadata_order() {
        let cube = |name: &str| CubeMeta {
            name: name.to_string(),
            description: None,
            sql_schemas: Some(vec!["sales".to_string(), "finance".to_string()]),
            title: None,
            r#type: CubeMetaType::Cube,
            dimensions: vec![],
            measures: vec![],
            segments: vec![],
            joins: None,
            folders: None,
            nested_folders: None,
            hierarchies: None,
            meta: None,
        };
        let projections = |cubes| {
            MetaContext::new(cubes, HashMap::new(), HashMap::new(), Uuid::new_v4())
                .catalog_projections
                .into_iter()
                .map(|projection| (projection.schema, projection.name, projection.oid))
                .collect::<Vec<_>>()
        };

        assert_eq!(
            projections(vec![cube("date"), cube("orders")]),
            projections(vec![cube("orders"), cube("date")]),
        );
    }
}
