MODEL viseta_test7.customer_lifetime_value
(
  kind INCREMENTAL_BY_UNIQUE_KEY (unique_key user_id lookback 2),
  owner 'customer_analytics',
  tags ('lifetime_value', 'customer', 'orders'),
  audits (
    assert_not_null(user_id),
    assert_not_null(created_at)
  )
) AS
WITH stage_orders AS (
  SELECT
    user_id,
    id AS order_id,
    created_at,
    COALESCE(grand_total, 0) AS grand_total
  FROM orders
  WHERE user_id IS NOT NULL
    AND created_at IS NOT NULL
    AND created_at >= CURRENT_DATE - INTERVAL '30' DAY
),
transform_aggregates AS (
  SELECT
    user_id,
    COUNT(order_id) AS count_of_orders,
    SUM(grand_total) AS life_time_value,
    MIN(created_at) AS first_order_date
  FROM stage_orders
  GROUP BY user_id
),
final_with_windows AS (
  SELECT
    user_id,
    count_of_orders,
    life_time_value,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY first_order_date) AS row_num,
    PERCENT_RANK() OVER (ORDER BY life_time_value) AS life_time_value_percent_rank,
    AVG(life_time_value) OVER (ORDER BY user_id ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS life_time_value_30d_avg,
    LAG(life_time_value, 1) OVER (PARTITION BY user_id ORDER BY user_id) AS life_time_value_lag_1,
    LAG(count_of_orders, 1) OVER (PARTITION BY user_id ORDER BY user_id) AS count_of_orders_lag_1
  FROM transform_aggregates
)
SELECT
  user_id,
  life_time_value,
  count_of_orders,
  life_time_value_percent_rank,
  life_time_value_30d_avg,
  life_time_value_lag_1,
  count_of_orders_lag_1
FROM final_with_windows
WHERE user_id IS NOT NULL;
