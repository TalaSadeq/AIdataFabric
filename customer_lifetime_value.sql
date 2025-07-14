MODEL (
  name 'viseta_test7.customer_lifetime_value',
  owner 'customer_analytics',
  cron '0 2 * * *',
  kind INCREMENTAL_BY_UNIQUE_KEY unique_key user_id lookback 5,
  partitioned_by (user_id),
  tags ('customers', 'lifetime_value', 'orders'),
  audits (
    assert exists(user_id) as 'user_id must not be null'
  )
) AS
WITH
  stage_orders AS (
    SELECT
      id AS order_id,
      user_id,
      grand_total,
      created_at
    FROM
      orders
    WHERE
      user_id IS NOT NULL
  ),
  transform_customer_orders AS (
    SELECT
      user_id,
      COUNT(order_id) AS count_of_orders,
      SUM(grand_total) AS life_time_value
    FROM
      stage_orders
    GROUP BY
      user_id
  ),
  final_customers AS (
    SELECT
      u.id AS user_id,
      u.name AS user_name,
      COALESCE(tc.count_of_orders, 0) AS count_of_orders,
      COALESCE(tc.life_time_value, 0) AS life_time_value
    FROM
      users u
    LEFT JOIN
      transform_customer_orders tc ON u.id = tc.user_id
    WHERE
      u.id IS NOT NULL
  )
SELECT
  user_id,
  user_name,
  count_of_orders,
  life_time_value
FROM
  final_customers;