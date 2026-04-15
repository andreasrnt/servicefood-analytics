{{ config(
    materialized = 'table',
    partition_by = {
        'field': 'order_date',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by = ['channel', 'customer_segment', 'order_status']
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

, users AS (
    SELECT
          customer_id
        , customer_segment
        , r_score
        , f_score
        , m_score
        , rfm_total
        , recency_days
        , first_channel
        , first_campaign
        , first_order_date
        , last_order_date
    FROM {{ ref('dim_users') }}
)

, fx AS (
    SELECT
          period
        , lcy
        , usd_to_lcy
        , lcy_to_usd
    FROM {{ ref('stg_exchange_rates') }}
)

, churn_risk AS (
    SELECT
          o.customer_id
        , CASE
            WHEN u.customer_segment IN ('VIP', 'REGULAR')
                 AND SAFE_DIVIDE(
                     COUNTIF(o.is_sla_breach = TRUE),
                     COUNT(*)
                 ) >= 0.40
                 AND COUNTIF(o.is_sla_breach = TRUE) >= 2 THEN 'HIGH'
            WHEN SAFE_DIVIDE(
                     COUNTIF(o.is_sla_breach = TRUE),
                     COUNT(*)
                 ) >= 0.30
                 AND COUNTIF(o.is_sla_breach = TRUE) >= 2 THEN 'MEDIUM'
            ELSE 'LOW'
          END AS churn_risk_tier
    FROM orders o
    LEFT JOIN users u ON o.customer_id = u.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY 1, u.customer_segment
)

, joined AS (
    SELECT
          o.order_id
        , o.session_id
        , o.customer_id
        , o.restaurant_id
        , o.courier_id
        , o.order_date
        , o.placed_at
        , o.order_status
        , o.order_type
        , o.order_type_name
        , o.channel
        , o.campaign
        , o.voucher_code
        , o.device
        , o.city
        , o.destination_name
        , o.delivery_distance_km
        , o.time_slot
        , o.is_weekend
        , o.is_peak_hour
        , o.order_hour
        , o.cancel_note
        , o.is_cancelled_by_driver
        , o.is_cancelled_by_restaurant
        , o.is_cancelled_by_customer
        , o.is_first_order
        , o.user_order_number
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
        , u.customer_segment
        , u.r_score
        , u.f_score
        , u.m_score
        , u.rfm_total
        , u.recency_days
        , u.first_channel
        , u.first_campaign
        , u.first_order_date
        , u.last_order_date
        , COALESCE(cr.churn_risk_tier, 'LOW') AS churn_risk_tier
        , FORMAT_DATE('%Y-%m', o.order_date) AS order_period
        , fx.lcy
        , fx.usd_to_lcy
        , fx.lcy_to_usd
    FROM orders o
    LEFT JOIN users u ON o.customer_id = u.customer_id
    LEFT JOIN churn_risk cr ON o.customer_id = cr.customer_id
    LEFT JOIN fx ON FORMAT_DATE('%Y-%m', o.order_date) = fx.period
)

SELECT
      order_id
    , session_id
    , customer_id
    , restaurant_id
    , courier_id
    , order_date
    , order_period
    , placed_at
    , order_status
    , order_type
    , order_type_name
    , channel
    , campaign
    , voucher_code
    , device
    , city
    , destination_name
    , delivery_distance_km
    , time_slot
    , is_weekend
    , is_peak_hour
    , order_hour
    , cancel_note
    , is_cancelled_by_driver
    , is_cancelled_by_restaurant
    , is_cancelled_by_customer
    , is_first_order
    , user_order_number
    , prep_time_mins
    , courier_wait_mins
    , courier_ride_mins
    , total_delivery_mins
    , is_sla_breach
    , delivery_bucket
    , dominant_bottleneck
    , customer_segment
    , churn_risk_tier
    , r_score
    , f_score
    , m_score
    , rfm_total
    , recency_days
    , first_channel
    , first_campaign
    , first_order_date
    , last_order_date
    , lcy
    , usd_to_lcy
    , lcy_to_usd
    , food_price AS food_price_lcy
    , discount_amount AS discount_amount_lcy
    , base_delivery_price AS base_delivery_price_lcy
    , delivery_price AS delivery_price_lcy
    , platform_fee AS platform_fee_lcy
    , restaurant_fee AS restaurant_fee_lcy
    , courier_commission AS courier_commission_lcy
    , gmv AS gmv_lcy
    , net_revenue AS net_revenue_lcy
    , ROUND(food_price * lcy_to_usd, 2) AS food_price_usd
    , ROUND(discount_amount * lcy_to_usd, 2) AS discount_amount_usd
    , ROUND(base_delivery_price * lcy_to_usd, 2) AS base_delivery_price_usd
    , ROUND(delivery_price * lcy_to_usd, 2) AS delivery_price_usd
    , ROUND(platform_fee * lcy_to_usd, 2) AS platform_fee_usd
    , ROUND(restaurant_fee * lcy_to_usd, 2) AS restaurant_fee_usd
    , ROUND(courier_commission * lcy_to_usd, 2) AS courier_commission_usd
    , ROUND(gmv * lcy_to_usd, 2) AS gmv_usd
    , ROUND(net_revenue * lcy_to_usd, 2) AS net_revenue_usd
FROM joined
;