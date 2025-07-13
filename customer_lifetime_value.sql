MODEL(
  name 'customer_lifetime_value'
  kind INCREMENTAL_BY_UNIQUE_KEY unique_key user_id lookback 5
  owner 'customer_analytics'
  cron '0 2 * * *'
  audits (
    assert_not_null_user_id CHECK (user_id IS NOT NULL)
  )
  tags (
    'customer', 'lifetime_value', 'orders'
  )
) AS
WITH
  stage_orders AS (
    SELECT
      id,
      user_id,
      grand_total,
      created_at
    FROM default.orders
    WHERE user_id IS NOT NULL
  ),
  transform_aggregations AS (
    SELECT
      user_id,
      COALESCE(SUM(grand_total), 0) AS life_time_value,
      COUNT(id) AS count_of_orders
    FROM stage_orders
    GROUP BY user_id
  ),
  final AS (
    SELECT
      u.id AS user_id,
      u.name AS user_name,
      t.life_time_value,
      t.count_of_orders
    FROM transform_aggregations t
    JOIN default.users u ON u.id = t.user_id
  )
SELECT user_id, user_name, life_time_value, count_of_orders FROM final;