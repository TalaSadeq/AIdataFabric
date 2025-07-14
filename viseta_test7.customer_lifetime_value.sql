MODEL (
  name 'viseta_test7.customer_lifetime_value',
  owner 'customer_analytics',
  cron '0 2 * * *',
  kind INCREMENTAL_BY_UNIQUE_KEY (unique_key user_id, lookback 2),
  partitioned_by (user_id),
  tags ('customers', 'lifetime_value', 'orders'),
  audits (
    assert exists(user_id) AS 'user_id must not be null',
    assert exists(created_at) AS 'created_at must not be null'
  )
) AS
WITH
  stage_orders AS (
    SELECT
      id AS order_id,
      user_id,
      grand_total,
      created_at
    FROM orders
    WHERE user_id IS NOT NULL
      AND created_at IS NOT NULL
  ),
  transform_customer_orders AS (
    SELECT
      user_id,
      COUNT(order_id) AS count_of_orders,
      SUM(grand_total) AS life_time_value
    FROM stage_orders
    GROUP BY user_id
  ),
  final_customers AS (
    SELECT
      u.id AS user_id,
      u.name AS user_name,
      COALESCE(tc.count_of_orders, 0) AS count_of_orders,
      COALESCE(tc.life_time_value, 0) AS life_time_value,
      ROW_NUMBER() OVER (PARTITION BY u.id ORDER BY MAX(so.created_at) DESC NULLS LAST) AS order_rank,
      PERCENT_RANK() OVER (ORDER BY COALESCE(tc.life_time_value, 0)) AS life_time_value_percent_rank,
      AVG(COALESCE(tc.life_time_value, 0)) OVER (PARTITION BY u.id ORDER BY MAX(so.created_at) ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS life_time_value_30d_moving_avg,
      LAG(COALESCE(tc.life_time_value, 0), 1) OVER (PARTITION BY u.id ORDER BY MAX(so.created_at)) AS life_time_value_prev_period,
      LAG(COALESCE(tc.count_of_orders, 0), 1) OVER (PARTITION BY u.id ORDER BY MAX(so.created_at)) AS count_of_orders_prev_period
    FROM users u
    LEFT JOIN stage_orders so ON u.id = so.user_id
    LEFT JOIN transform_customer_orders tc ON u.id = tc.user_id
    WHERE u.id IS NOT NULL
    GROUP BY u.id, u.name, tc.count_of_orders, tc.life_time_value
  )
SELECT
  user_id,
  user_name,
  count_of_orders,
  life_time_value,
  order_rank,
  life_time_value_percent_rank,
  life_time_value_30d_moving_avg,
  life_time_value_prev_period,
  count_of_orders_prev_period
FROM final_customers