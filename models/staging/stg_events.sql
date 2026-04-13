WITH source AS (
    SELECT * FROM {{ ref('events') }}
)

SELECT
      CAST(event_id AS INT64) AS event_id
    , CAST(session_id AS INT64) AS session_id
    , CAST(user_id AS INT64) AS user_id
    , CAST(event_timestamp AS TIMESTAMP) AS event_timestamp
    , DATE(CAST(event_timestamp AS TIMESTAMP)) AS event_date
    , event_name
    , page_name
    , channel
    , NULLIF(campaign, '') AS campaign
    , device
    , city
    , NULLIF(voucher_code, '') AS voucher_code
    , CASE
        WHEN event_name = 'page_view' AND page_name = 'home' THEN 'APP_VISIT'
        WHEN event_name IN ('page_view', 'restaurant_click', 'search_query')
             AND page_name IN ('restaurant_listing', 'search') THEN 'BROWSE'
        WHEN event_name IN ('menu_impression', 'scroll_depth')
             AND page_name = 'restaurant_detail' THEN 'BROWSE'
        WHEN event_name IN ('add_to_cart', 'cart_view') THEN 'ADD_TO_CART'
        WHEN event_name IN ('voucher_applied', 'checkout_start', 'payment_page_view') THEN 'CHECKOUT'
        WHEN event_name = 'order_placed' THEN 'BOOKING_ACCEPTED'
        WHEN event_name = 'order_ready_notification' THEN 'FOOD_READY_PICKUP'
        WHEN event_name = 'order_delivered' THEN 'DELIVERED'
        ELSE 'NOISE'
      END AS funnel_stage
    , CASE
        WHEN event_name = 'page_view' AND page_name = 'home' THEN 1
        WHEN event_name IN ('page_view', 'restaurant_click', 'search_query', 'menu_impression', 'scroll_depth') THEN 2
        WHEN event_name IN ('add_to_cart', 'cart_view') THEN 3
        WHEN event_name IN ('voucher_applied', 'checkout_start', 'payment_page_view') THEN 4
        WHEN event_name = 'order_placed' THEN 5
        WHEN event_name = 'order_ready_notification' THEN 6
        WHEN event_name = 'order_delivered' THEN 7
        ELSE 0
      END AS stage_order
FROM source
;