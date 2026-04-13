{{ config(
    materialized = 'table',
    partition_by = {
        'field': 'session_date',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by = ['channel', 'device']
) }}

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
    WHERE funnel_stage != 'NOISE'
)

, stage_counts AS (
    SELECT
          event_date AS session_date
        , channel
        , COALESCE(campaign, 'NONE') AS campaign
        , device
        , city
        , COUNTIF(funnel_stage = 'APP_VISIT') AS s1_app_visit
        , COUNTIF(funnel_stage = 'BROWSE') AS s2_browse
        , COUNTIF(funnel_stage = 'ADD_TO_CART') AS s3_add_to_cart
        , COUNTIF(funnel_stage = 'CHECKOUT') AS s4_checkout
        , COUNTIF(funnel_stage = 'BOOKING_ACCEPTED') AS s5_booking_accepted
        , COUNTIF(funnel_stage = 'FOOD_READY_PICKUP') AS s6_food_ready_pickup
        , COUNTIF(funnel_stage = 'DELIVERED') AS s7_delivered
    FROM events
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
      session_date
    , channel
    , campaign
    , device
    , city

    , s1_app_visit
    , s2_browse
    , s3_add_to_cart
    , s4_checkout
    , s5_booking_accepted
    , s6_food_ready_pickup
    , s7_delivered

    , SAFE_DIVIDE(s2_browse, s1_app_visit) AS cvr_visit_to_browse
    , SAFE_DIVIDE(s3_add_to_cart, s2_browse) AS cvr_browse_to_cart
    , SAFE_DIVIDE(s4_checkout, s3_add_to_cart) AS cvr_cart_to_checkout
    , SAFE_DIVIDE(s5_booking_accepted, s4_checkout) AS cvr_checkout_to_booking
    , SAFE_DIVIDE(s6_food_ready_pickup, s5_booking_accepted) AS cvr_booking_to_pickup
    , SAFE_DIVIDE(s7_delivered, s6_food_ready_pickup) AS cvr_pickup_to_delivered
    , SAFE_DIVIDE(s7_delivered, s1_app_visit) AS cvr_end_to_end

    , s1_app_visit - s2_browse AS drop_visit_to_browse
    , s2_browse - s3_add_to_cart AS drop_browse_to_cart
    , s3_add_to_cart - s4_checkout AS drop_cart_to_checkout
    , s4_checkout - s5_booking_accepted AS drop_checkout_to_booking
    , s5_booking_accepted - s6_food_ready_pickup AS drop_booking_to_pickup
    , s6_food_ready_pickup - s7_delivered AS drop_pickup_to_delivered

    , ROUND(1 - SAFE_DIVIDE(s2_browse, s1_app_visit), 4) AS dropoff_visit_to_browse
    , ROUND(1 - SAFE_DIVIDE(s3_add_to_cart, s2_browse), 4) AS dropoff_browse_to_cart
    , ROUND(1 - SAFE_DIVIDE(s4_checkout, s3_add_to_cart), 4) AS dropoff_cart_to_checkout
    , ROUND(1 - SAFE_DIVIDE(s5_booking_accepted, s4_checkout), 4) AS dropoff_checkout_to_booking
    , ROUND(1 - SAFE_DIVIDE(s6_food_ready_pickup, s5_booking_accepted), 4) AS dropoff_booking_to_pickup
    , ROUND(1 - SAFE_DIVIDE(s7_delivered, s6_food_ready_pickup), 4) AS dropoff_pickup_to_delivered

FROM stage_counts
;