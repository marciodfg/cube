cube(`schema_orders`, {
  sql: `
    SELECT 1 AS id, 100 AS amount, 'new' AS status
    UNION ALL
    SELECT 2 AS id, 200 AS amount, 'processed' AS status
  `,
  sqlSchemas: [`sales`, `finance`],
  measures: {
    count: {
      type: `count`,
    },
    totalAmount: {
      sql: `amount`,
      type: `sum`,
    },
  },
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },
    amount: {
      sql: `amount`,
      type: `number`,
    },
    status: {
      sql: `status`,
      type: `string`,
    },
  },
});

view(`schema_orders_view`, {
  sqlSchemas: [`sales`, `finance`],
  cubes: [{
    joinPath: schema_orders,
    includes: [`id`, `amount`, `status`, `count`, `totalAmount`],
  }],
});

cube(`legacy_orders`, {
  sql: `SELECT 1 AS id UNION ALL SELECT 2 AS id`,
  measures: {
    count: {
      type: `count`,
    },
  },
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },
  },
});

cube(`hidden_orders`, {
  sql: `SELECT 1 AS id`,
  public: false,
  sqlSchemas: [`hidden_only`],
  measures: {
    count: {
      type: `count`,
    },
  },
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },
  },
});
