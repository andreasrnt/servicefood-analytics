-- Returns rows when test FAILS
-- Each block checks one mart's primary key for uniqueness
-- Test passes only when this query returns 0 rows

-- fact_orders: order_id
SELECT
      'fact_orders'
    , 'order_id'
    , 'duplicate'
    , CAST(order_id AS STRING)
FROM {{ ref('fact_orders') }}
GROUP BY order_id
HAVING COUNT(*) > 1

UNION ALL

-- fact_delivery: order_id
SELECT
      'fact_delivery'
    , 'order_id'
    , 'duplicate'
    , CAST(order_id AS STRING)
FROM {{ ref('fact_delivery') }}
GROUP BY order_id
HAVING COUNT(*) > 1

UNION ALL

-- fact_user_metrics: order_id
SELECT
      'fact_user_metrics'
    , 'order_id'
    , 'duplicate'
    , CAST(order_id AS STRING)
FROM {{ ref('fact_user_metrics') }}
GROUP BY order_id
HAVING COUNT(*) > 1

UNION ALL

-- dim_users: customer_id
SELECT
      'dim_users'
    , 'customer_id'
    , 'duplicate'
    , CAST(customer_id AS STRING)
FROM {{ ref('dim_users') }}
GROUP BY customer_id
HAVING COUNT(*) > 1

UNION ALL

-- dim_restaurants: restaurant_id
SELECT
      'dim_restaurants'
    , 'restaurant_id'
    , 'duplicate'
    , CAST(restaurant_id AS STRING)
FROM {{ ref('dim_restaurants') }}
GROUP BY restaurant_id
HAVING COUNT(*) > 1

UNION ALL

-- dim_couriers: courier_id
SELECT
      'dim_couriers'
    , 'courier_id'
    , 'duplicate'
    , CAST(courier_id AS STRING)
FROM {{ ref('dim_couriers') }}
GROUP BY courier_id
HAVING COUNT(*) > 1

UNION ALL

-- fact_funnel: composite key (session_date, channel, campaign, device, city)
SELECT
      'fact_funnel'
    , 'session_date+channel+campaign+device+city'
    , 'duplicate'
    , CONCAT(
          CAST(session_date AS STRING), '|'
        , COALESCE(channel, 'NULL'), '|'
        , COALESCE(campaign, 'NULL'), '|'
        , COALESCE(device, 'NULL'), '|'
        , COALESCE(city, 'NULL')
      )
FROM {{ ref('fact_funnel') }}
GROUP BY session_date, channel, campaign, device, city
HAVING COUNT(*) > 1

UNION ALL

-- fact_channel_performance: composite key (channel, campaign, voucher_code)
SELECT
      'fact_channel_performance'
    , 'channel+campaign+voucher_code'
    , 'duplicate'
    , CONCAT(
          COALESCE(channel, 'NULL'), '|'
        , COALESCE(campaign, 'NULL'), '|'
        , COALESCE(voucher_code, 'NULL')
      )
FROM {{ ref('fact_channel_performance') }}
GROUP BY channel, campaign, voucher_code
HAVING COUNT(*) > 1