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
WITH stage_customers AS (
  SELECT
    customer_id
  FROM customers
  WHERE
    customer_id IS NOT NULL
),

stage_order_payments AS (
  SELECT
    o.customer_id,
    o.order_id,
    p.payment_value
  FROM orders AS o
  JOIN order_payments AS p
    ON o.order_id = p.order_id
  WHERE
    o.customer_id IS NOT NULL
    AND o.order_status IN ('delivered', 'approved')
    AND p.payment_value > 0
),

/*
Transform CTEs: Perform aggregations to calculate business metrics.
*/
transform_customer_aggregates AS (
  SELECT
    customer_id,
    SUM(COALESCE(payment_value, 0)) AS life_time_value,
    COUNT(DISTINCT order_id) AS count_of_orders
  FROM stage_order_payments
  GROUP BY
    customer_id
),

/*
Final CTE: Join customer data with aggregated order metrics to create the complete view.
*/
final AS (
  SELECT
    sc.customer_id,
    COALESCE(tca.life_time_value, 0) AS life_time_value,
    COALESCE(tca.count_of_orders, 0) AS count_of_orders
  FROM stage_customers AS sc
  LEFT JOIN transform_customer_aggregates AS tca
    ON sc.customer_id = tca.customer_id
)

/*
Select all columns from the final CTE to produce the model's output.
*/
SELECT
  customer_id,
  life_time_value,
  count_of_orders
FROM final;