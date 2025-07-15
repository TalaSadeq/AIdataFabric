MODEL (
  name viseta_test7.car_model_orders,
  owner 'customer_analytics',
  cron '0 2 * * *',
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key model_name,
    lookback 2
  ),
  grain 'One car model',
  tags ('customer_analytics', 'orders', 'cars')
);

-- Data Quality Audits
AUDIT (
  name not_null_model_name,
  not_null(
    column = model_name
  )
);

AUDIT (
  name positive_life_time_value,
  assert(
    query = 'life_time_value >= 0',
    description = 'Ensures that the calculated lifetime value is not negative.'
  )
);

AUDIT (
  name positive_order_count,
  assert(
    query = 'count_of_orders > 0',
    description = 'Ensures that each car model has at least one order.'
  )
);

-- CTE for staging orders, filtered for the incremental window
WITH stage_orders AS (
  SELECT
    id AS order_id,
    grand_total,
    car_id,
    user_id,
    created_at
  FROM orders
  WHERE
    created_at BETWEEN @start_date AND @end_date
),

-- CTE for staging car data
-- NOTE: The 'cars' table schema is missing the 'model_id' column required to join with 'models'.
-- This model assumes 'model_id' exists in the source 'cars' table.
stage_cars AS (
  SELECT
    id AS car_id,
    model_id -- Assumed column
  FROM cars
),

-- CTE for staging model data
stage_models AS (
  SELECT
    id AS model_id,
    name_en
  FROM models
),

-- Transformation CTE to join sources and apply data quality filters
transform_data AS (
  SELECT
    o.order_id,
    m.name_en,
    COALESCE(o.grand_total, 0.0) AS grand_total
  FROM stage_orders AS o
  INNER JOIN
    stage_cars AS c
    ON o.car_id = c.car_id
  INNER JOIN
    stage_models AS m
    ON c.model_id = m.model_id
  WHERE
    -- Enforcing 'must_have_data' rules from the data product description
    o.user_id IS NOT NULL
    AND o.car_id IS NOT NULL
    AND m.name_en IS NOT NULL
    AND o.order_id IS NOT NULL
),

-- Final CTE to aggregate results by car model
final AS (
  SELECT
    name_en AS model_name,
    SUM(grand_total) AS life_time_value,
    COUNT(order_id) AS count_of_orders
  FROM transform_data
  GROUP BY
    name_en
)

-- Final model output
SELECT
  model_name,
  life_time_value,
  count_of_orders
FROM final;