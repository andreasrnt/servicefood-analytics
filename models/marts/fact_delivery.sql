{{ config(
    materialized = 'table',
    partition_by = {
        'field': 'order_date',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by = ['restaurant_id', 'courier_id', 'customer_id']
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

, restaurants AS (
    SELECT
          restaurant_id
        , restaurant_name
        , cuisine_type
        , is_partner
        , rating
    FROM {{ ref('stg_restaurants') }}
)

, couriers AS (
    SELECT
          courier_id
        , vehicle_type
        , courier_tier
        , rating
    FROM {{ ref('stg_couriers') }}
)

, users AS (
    SELECT
          customer_id
        , customer_segment
        , churn_risk_tier
    FROM {{ ref('fact_user_metrics') }}
)

, joined AS (
    SELECT
          o.order_id
        , o.customer_id
        , o.restaurant_id
        , o.courier_id
        , o.order_date
        , o.placed_at
        , o.food_ready_at
        , o.picked_up_at
        , o.delivered_at
        , o.order_status
        , o.cancel_note
        , o.is_cancelled_by_driver
        , o.is_cancelled_by_restaurant
        , o.is_cancelled_by_customer
        , o.channel
        , o.city
        , o.destination_name
        , o.delivery_distance_km
        , o.order_type
        , o.time_slot
        , o.is_weekend
        , o.is_peak_hour
        , o.prep_time_mins
        , o.courier_wait_mins
        , o.courier_ride_mins
        , o.total_delivery_mins
        , o.is_sla_breach
        , o.delivery_bucket
        , o.dominant_bottleneck
        , r.restaurant_name
        , r.cuisine_type
        , r.is_partner AS is_partner_restaurant
        , r.rating AS restaurant_rating
        , c.vehicle_type AS courier_vehicle
        , c.courier_tier
        , c.rating AS courier_rating
        , u.customer_segment
        , u.churn_risk_tier
    FROM orders o
    LEFT JOIN restaurants r ON o.restaurant_id = r.restaurant_id
    LEFT JOIN couriers c ON o.courier_id = c.courier_id
    LEFT JOIN users u ON o.customer_id = u.customer_id
)

, party_avgs AS (
    SELECT
          *
        , ROUND(
            AVG(prep_time_mins) OVER (PARTITION BY restaurant_id)
          , 1) AS restaurant_avg_prep_mins
        , ROUND(
            AVG(courier_ride_mins) OVER (PARTITION BY courier_id)
          , 1) AS courier_avg_ride_mins
    FROM joined
    WHERE order_status = 'delivered'
)

, final AS (
    SELECT
          *
        , ROUND(30 + (delivery_distance_km * 3), 1) AS sla_threshold_mins
        , delivery_distance_km > 7 AS is_long_distance
        , CASE
            WHEN is_sla_breach = TRUE
            THEN ROUND(total_delivery_mins - (30 + (delivery_distance_km * 3)), 1)
          END AS mins_over_threshold
        , dominant_bottleneck = 'RESTAURANT_PREP' AS is_prep_bottleneck
        , dominant_bottleneck IN ('COURIER_WAIT', 'COURIER_RIDE') AS is_courier_bottleneck
        , ROUND(SAFE_DIVIDE(prep_time_mins, total_delivery_mins), 4) AS prep_share_pct
        , ROUND(SAFE_DIVIDE(courier_wait_mins, total_delivery_mins), 4) AS wait_share_pct
        , ROUND(SAFE_DIVIDE(courier_ride_mins, total_delivery_mins), 4) AS ride_share_pct
        , ROUND(prep_time_mins - restaurant_avg_prep_mins, 1) AS prep_vs_restaurant_avg
        , ROUND(courier_ride_mins - courier_avg_ride_mins, 1) AS ride_vs_courier_avg
    FROM party_avgs
)

SELECT
      order_id
    , customer_id
    , restaurant_id
    , courier_id
    , order_date
    , placed_at
    , food_ready_at
    , picked_up_at
    , delivered_at
    , order_status
    , cancel_note
    , is_cancelled_by_driver
    , is_cancelled_by_restaurant
    , is_cancelled_by_customer
    , channel
    , city
    , destination_name
    , delivery_distance_km
    , is_long_distance
    , order_type
    , time_slot
    , is_weekend
    , is_peak_hour
    , restaurant_name
    , cuisine_type
    , is_partner_restaurant
    , restaurant_rating
    , courier_vehicle
    , courier_tier
    , courier_rating
    , customer_segment
    , churn_risk_tier
    , prep_time_mins
    , courier_wait_mins
    , courier_ride_mins
    , total_delivery_mins
    , sla_threshold_mins
    , is_sla_breach
    , mins_over_threshold
    , delivery_bucket
    , dominant_bottleneck
    , is_prep_bottleneck
    , is_courier_bottleneck
    , prep_share_pct
    , wait_share_pct
    , ride_share_pct
    , restaurant_avg_prep_mins
    , prep_vs_restaurant_avg
    , courier_avg_ride_mins
    , ride_vs_courier_avg
FROM final
;