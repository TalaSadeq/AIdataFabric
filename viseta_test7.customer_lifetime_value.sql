MODEL (
  name viseta_test7.customer_lifetime_value,
  owner 'customer_analytics',
  cron '0 2 * * *',
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key customer_id,
    lookback 5
  ),
  grain customer_id,
  tags (
    'customer_analytics',
    'ltv',
    'retention'
  )
);

AUDIT (
  NOT_NULL,
  columns (customer_id)
);

AUDIT (
  POSITIVE_VALUES,
  columns (life_time_value, count_of_orders)
);

/*
Stage CTEs: Select and filter source data to ensure data quality and relevance.
*/
WITH stage_orders AS (
  SELECT
    customer_id,
    order_id,
    grand_total -- Assumed column based on `what_to_calculate`.
  FROM raw.orders
  WHERE
    customer_id IS NOT NULL
),

stage_customers AS (
  SELECT
    customer_id
  FROM raw.customers
  WHERE
    customer_id IS NOT NULL
),

/*
Transform CTEs: Perform aggregations to calculate business metrics.
*/
transform_order_aggregates AS (
  SELECT
    customer_id,
    SUM(COALESCE(grand_total, 0)) AS life_time_value,
    COUNT(order_id) AS count_of_orders
  FROM stage_orders
  GROUP BY
    customer_id
),

/*
Final CTE: Join customer data with aggregated order metrics to create the complete view.
*/
final AS (
  SELECT
    sc.customer_id,
    COALESCE(toa.life_time_value, 0) AS life_time_value,
    COALESCE(toa.count_of_orders, 0) AS count_of_orders
  FROM stage_customers AS sc
  LEFT JOIN transform_order_aggregates AS toa
    ON sc.customer_id = toa.customer_id
)

/*
Select all columns from the final CTE to produce the model's output.
*/
SELECT
  customer_id,
  life_time_value,
  count_of_orders
FROM final;