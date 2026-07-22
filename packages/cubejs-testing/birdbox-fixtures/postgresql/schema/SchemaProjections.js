cube(`schema_orders`, {
  description: `Orders exposed to department-specific SQL schemas`,
  sql: `
    SELECT 1 AS id, 10 AS customer_id, 100 AS amount, 'new' AS status
    UNION ALL
    SELECT 2 AS id, 20 AS customer_id, 200 AS amount, 'processed' AS status
  `,
  sqlSchemas: [`sales`, `finance`],
  joins: {
    schema_customers: {
      sql: `${CUBE}.customer_id = ${schema_customers}.id`,
      relationship: `many_to_one`,
    },
  },
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
      description: `Order identifier`,
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },
    amount: {
      sql: `amount`,
      type: `number`,
    },
    customerId: {
      sql: `customer_id`,
      type: `number`,
    },
    status: {
      sql: `status`,
      type: `string`,
    },
  },
});

cube(`schema_customers`, {
  sql: `
    SELECT 10 AS id, 'Ada' AS name
    UNION ALL
    SELECT 20 AS id, 'Grace' AS name
  `,
  sqlSchemas: [`public`, `finance`],
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
    name: {
      sql: `name`,
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

view(`sales_legacy_orders`, {
  sqlSchemas: [`sales`],
  cubes: [{
    joinPath: legacy_orders,
    includes: [`id`, `count`],
  }],
});

cube(`shared_date`, {
  sql: `SELECT '2026-07-23'::date AS date`,
  sqlSchemas: [`public`, `sales`],
  measures: {
    count: {
      type: `count`,
    },
  },
  dimensions: {
    date: {
      sql: `date`,
      type: `time`,
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
