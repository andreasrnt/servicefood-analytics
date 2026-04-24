-- Returns rows when test FAILS
-- Each block checks one mart's primary key for not_null
-- Test passes only when this query returns 0 rows

-- fact_orders: order_id
SELECT
      'fact_orders' AS model
    , 'order_id' AS column_name
    , 'null' AS failure_type
    , CAST(order_id AS STRING) AS failed_value
FROM {{ ref('fact_orders') }}
WHERE order_id IS NULL

UNION ALL

-- fact_delivery: order_id
SELECT
      'fact_delivery'
    , 'order_id'
    , 'null'
    , CAST(order_id AS STRING)
FROM {{ ref('fact_delivery') }}
WHERE order_id IS NULL

UNION ALL

-- fact_user_metrics: order_id
SELECT
      'fact_user_metrics'
    , 'order_id'
    , 'null'
    , CAST(order_id AS STRING)
FROM {{ ref('fact_user_metrics') }}
WHERE order_id IS NULL

UNION ALL

-- dim_users: customer_id
SELECT
      'dim_users'
    , 'customer_id'
    , 'null'
    , CAST(customer_id AS STRING)
FROM {{ ref('dim_users') }}
WHERE customer_id IS NULL

UNION ALL

-- dim_restaurants: restaurant_id
SELECT
      'dim_restaurants'
    , 'restaurant_id'
    , 'null'
    , CAST(restaurant_id AS STRING)
FROM {{ ref('dim_restaurants') }}
WHERE restaurant_id IS NULL

UNION ALL

-- dim_couriers: courier_id
SELECT
      'dim_couriers'
    , 'courier_id'
    , 'null'
    , CAST(courier_id AS STRING)
FROM {{ ref('dim_couriers') }}
WHERE courier_id IS NULL

UNION ALL

-- fact_funnel: composite key (session_date, channel, campaign, device, city)
SELECT
      'fact_funnel'
    , 'session_date+channel+campaign+device+city'
    , 'null'
    , CONCAT(
          CAST(session_date AS STRING), '|'
        , COALESCE(channel, 'NULL'), '|'
        , COALESCE(campaign, 'NULL'), '|'
        , COALESCE(device, 'NULL'), '|'
        , COALESCE(city, 'NULL')
      )
FROM {{ ref('fact_funnel') }}
WHERE session_date IS NULL
   OR channel IS NULL
   OR device IS NULL
   OR city IS NULL

UNION ALL

-- fact_channel_performance: composite key (channel, campaign, voucher_code)
SELECT
      'fact_channel_performance'
    , 'channel+campaign+voucher_code'
    , 'null'
    , CONCAT(
          COALESCE(channel, 'NULL'), '|'
        , COALESCE(campaign, 'NULL'), '|'
        , COALESCE(voucher_code, 'NULL')
      )
FROM {{ ref('fact_channel_performance') }}
WHERE channel IS NULL