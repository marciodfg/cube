use std::{any::Any, collections::BTreeMap, sync::Arc};

use async_trait::async_trait;

use crate::transport::{CatalogProjection, CATALOG_PUBLIC_SCHEMA_OID};
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, ListBuilder, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

// https://github.com/postgres/postgres/blob/REL_16_4/src/include/catalog/pg_namespace.dat#L15-L17
pub const PG_NAMESPACE_CATALOG_OID: u32 = 11;
// https://github.com/postgres/postgres/blob/REL_16_4/src/include/catalog/pg_namespace.dat#L18-L20
pub const PG_NAMESPACE_TOAST_OID: u32 = 99;
// https://github.com/postgres/postgres/blob/REL_16_4/src/include/catalog/pg_namespace.dat#L21-L24
pub const PG_NAMESPACE_PUBLIC_OID: u32 = CATALOG_PUBLIC_SCHEMA_OID;

struct PgNamespace {
    oid: u32,
    nspname: String,
    nspowner: u32,
}

struct PgCatalogNamespaceBuilder {
    oid: UInt32Builder,
    nspname: StringBuilder,
    nspowner: UInt32Builder,
    nspacl: ListBuilder<StringBuilder>,
    xmin: UInt32Builder,
}

impl PgCatalogNamespaceBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            oid: UInt32Builder::new(capacity),
            nspname: StringBuilder::new(capacity),
            nspowner: UInt32Builder::new(capacity),
            nspacl: ListBuilder::new(StringBuilder::new(capacity)),
            xmin: UInt32Builder::new(capacity),
        }
    }

    fn add_namespace(&mut self, ns: &PgNamespace) {
        self.oid.append_value(ns.oid).unwrap();
        self.nspname.append_value(&ns.nspname).unwrap();
        self.nspowner.append_value(ns.nspowner).unwrap();
        self.nspacl.append(false).unwrap();
        self.xmin.append_value(1).unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.oid.finish()),
            Arc::new(self.nspname.finish()),
            Arc::new(self.nspowner.finish()),
            Arc::new(self.nspacl.finish()),
            Arc::new(self.xmin.finish()),
        ];

        columns
    }
}

pub struct PgCatalogNamespaceProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogNamespaceProvider {
    pub fn new(catalog_projections: &[CatalogProjection]) -> Self {
        let mut builder = PgCatalogNamespaceBuilder::new();
        let mut user_schemas: BTreeMap<_, _> = catalog_projections
            .iter()
            .map(|projection| (projection.schema.as_str(), projection.schema_oid))
            .collect();

        builder.add_namespace(&PgNamespace {
            oid: PG_NAMESPACE_CATALOG_OID,
            nspname: "pg_catalog".to_string(),
            nspowner: 10,
        });
        if let Some(public_oid) = user_schemas.remove("public") {
            builder.add_namespace(&PgNamespace {
                oid: public_oid,
                nspname: "public".to_string(),
                nspowner: 10,
            });
        }
        builder.add_namespace(&PgNamespace {
            oid: 13000,
            nspname: "information_schema".to_string(),
            nspowner: 10,
        });

        for (name, oid) in user_schemas {
            builder.add_namespace(&PgNamespace {
                oid,
                nspname: name.to_string(),
                nspowner: 10,
            });
        }

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, StringArray};

    use super::*;

    fn projection(schema: &str, schema_oid: u32) -> CatalogProjection {
        CatalogProjection {
            oid: 18000,
            record_oid: 18001,
            array_handler_oid: 18002,
            schema: schema.to_string(),
            schema_oid,
            name: "orders".to_string(),
            description: None,
            columns: vec![],
        }
    }

    fn namespace_names(provider: PgCatalogNamespaceProvider) -> Vec<String> {
        provider.data[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .flatten()
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn visible_user_namespaces_come_only_from_catalog_projections() {
        let empty_namespaces = namespace_names(PgCatalogNamespaceProvider::new(&[]));
        assert!(!empty_namespaces.contains(&"public".to_string()));

        let legacy_namespaces = namespace_names(PgCatalogNamespaceProvider::new(&[projection(
            "public",
            PG_NAMESPACE_PUBLIC_OID,
        )]));
        assert!(legacy_namespaces.contains(&"public".to_string()));
    }
}

#[async_trait]
impl TableProvider for PgCatalogNamespaceProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("nspname", DataType::Utf8, false),
            Field::new("nspowner", DataType::UInt32, false),
            Field::new(
                "nspacl",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("xmin", DataType::UInt32, false),
        ]))
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let batch = RecordBatch::try_new(self.schema(), self.data.to_vec())?;

        Ok(Arc::new(MemoryExec::try_new(
            &[vec![batch]],
            self.schema(),
            projection.clone(),
        )?))
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}
