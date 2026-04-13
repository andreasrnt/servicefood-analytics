{{ config(
    materialized = 'table',
    partition_by = {
        'field': 'order_date',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by = ['channel', 'city', 'order_status']
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

, restaurants AS (
    SELECT * FROM {{ ref('stg_restaurants') }}
)

, couriers AS (
    SELECT * FROM {{ ref('stg_couriers') }}
)

, users AS (
    SELECT
          customer_id
        , customer_segment
        , churn_risk_tier
    FROM {{ ref('fact_user_metrics') }}
)

SELECT
      o.order_id
    , o.session_id
    , o.customer_id
    , o.restaurant_id
    , o.courier_id
    , o.channel
    , o.campaign
    , o.voucher_code
    , o.device
    , o.city
    , o.destination_name
    , o.delivery_distance_km
    , o.order_type
    , o.order_type_name
    , o.placed_at
    , o.food_ready_at
    , o.picked_up_at
    , o.delivered_at
    , o.order_date
    , o.order_hour
    , o.is_weekend
    , o.is_peak_hour
    , o.time_slot
    , o.order_status
    , o.cancel_note
    , o.is_cancelled_by_driver
    , o.is_cancelled_by_restaurant
    , o.is_cancelled_by_customer
    , o.prep_time_mins
    , o.courier_wait_mins
    , o.courier_ride_mins
    , o.total_delivery_mins
    , o.is_sla_breach
    , o.delivery_bucket
    , o.dominant_bottleneck
    , o.food_price
    , o.discount_amount
    , o.base_delivery_price
    , o.delivery_price
    , o.platform_fee
    , o.restaurant_fee
    , o.courier_commission
    , o.gmv
    , o.net_revenue
    , o.user_order_number
    , o.is_first_order
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
;