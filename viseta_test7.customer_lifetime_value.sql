MODEL(
  name 'viseta_test7.customer_lifetime_value',
  owner 'customer_analytics',
  kind INCREMENTAL_BY_UNIQUE_KEY unique_key user_id, lookback 2,
  cron '0 2 * * *',
  audits (
    { name 'must_have_data_user_id', query 'SELECT user_id FROM orders WHERE user_id IS NULL LIMIT 1', condition 'ZERO_ROWS' },
    { name 'must_have_data_created_at', query 'SELECT created_at FROM orders WHERE created_at IS NULL LIMIT 1', condition 'ZERO_ROWS' }
  ),
  tags ('lifetime_value', 'customer', 'orders', 'retention')
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
),
transform_metrics AS (
  SELECT
    user_id,
    COUNT(order_id) AS count_of_orders,
    SUM(grand_total) AS life_time_value,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at) AS order_rank,
    SUM(SUM(grand_total)) OVER (PARTITION BY user_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_life_time_value,
    SUM(COUNT(order_id)) OVER (PARTITION BY user_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_of_orders,
    PERCENT_RANK() OVER (PARTITION BY user_id ORDER BY SUM(grand_total) DESC) AS life_time_value_percent_rank,
    AVG(SUM(grand_total)) OVER (PARTITION BY user_id ORDER BY created_at ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS life_time_value_moving_avg
  FROM stage_orders
  GROUP BY user_id, created_at
),
final AS (
  SELECT
    user_id,
    MAX(count_of_orders) AS count_of_orders,
    MAX(life_time_value) AS life_time_value,
    MAX(life_time_value_percent_rank) AS life_time_value_percent_rank,
    MAX(life_time_value_moving_avg) AS life_time_value_moving_avg
  FROM transform_metrics
  GROUP BY user_id
)
SELECT
  user_id AS id,
  count_of_orders,
  life_time_value,
  life_time_value_percent_rank,
  life_time_value_moving_avg
FROM final;