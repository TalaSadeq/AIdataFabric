MODEL (
  name WindAI_Payments,
  owner 'customer_analytics',
  cron '0 2 * * *',
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key customer_id,
    lookback 5
  ),
  grain customer_id,
  tags (
    'finance',
    'marketing',
    'customer'
  )
);

AUDIT (
  NOT_NULL,
  columns (customer_id)
);

AUDIT (
  POSITIVE_VALUES,
  columns (
    sum_of_orders,
    count_of_orders
  )
);

/*
Stage CTEs: Select and filter source data for relevance and quality.
*/
WITH stage_customers AS (
  SELECT
    customer_id
  FROM customers
  WHERE
    customer_id IS NOT NULL
),

stage_orders AS (
  SELECT
    o.customer_id,
    o.order_id,
    p.payment_value
  FROM orders AS o
  JOIN order_payments AS p
    ON o.order_id = p.order_id
  WHERE
    o.customer_id IS NOT NULL
    AND o.order_status NOT IN (
      'unavailable',
      'canceled'
    ) -- Filter for valid orders
),

/*
Transform CTEs: Aggregate data to calculate business metrics.
*/
transform_aggregates AS (
  SELECT
    customer_id,
    SUM(COALESCE(payment_value, 0)) AS sum_of_orders,
    COUNT(DISTINCT order_id) AS count_of_orders
  FROM stage_orders
  GROUP BY
    customer_id
),

/*
Final CTE: Join staged data to create the final model output.
*/
final AS (
  SELECT
    sc.customer_id,
    COALESCE(ta.sum_of_orders, 0) AS sum_of_orders,
    COALESCE(ta.count_of_orders, 0) AS count_of_orders
  FROM stage_customers AS sc
  LEFT JOIN transform_aggregates AS ta
    ON sc.customer_id = ta.customer_id
)

/*
Select all columns from the final CTE.
*/
SELECT
  customer_id,
  sum_of_orders,
  count_of_orders
FROM final;