{{ config(materialized = 'table') }}

WITH restaurants AS (
    SELECT * FROM {{ ref('stg_restaurants') }}
)

, order_stats AS (
    SELECT
          restaurant_id
        , COUNT(*) AS total_orders
        , COUNTIF(order_status = 'delivered') AS delivered_orders
        , COUNTIF(order_status != 'delivered') AS cancelled_orders
        , COUNTIF(is_cancelled_by_restaurant) AS cancelled_by_restaurant
        , ROUND(SAFE_DIVIDE(COUNTIF(order_status = 'delivered'), COUNT(*)), 4) AS fulfillment_rate
        , ROUND(AVG(CASE WHEN order_status = 'delivered' THEN prep_time_mins END), 1) AS avg_prep_time_mins
        , ROUND(AVG(CASE WHEN order_status = 'delivered' THEN total_delivery_mins END), 1) AS avg_total_delivery_mins
        , COUNTIF(is_sla_breach = TRUE) AS sla_breached_orders
        , ROUND(SAFE_DIVIDE(COUNTIF(is_sla_breach = TRUE), COUNTIF(order_status = 'delivered')), 4) AS sla_breach_rate
        , ROUND(SUM(CASE WHEN order_status = 'delivered' THEN food_price END), 0) AS total_food_revenue
        , ROUND(SUM(CASE WHEN order_status = 'delivered' THEN restaurant_fee END), 0) AS total_restaurant_fee_paid
        , COUNT(DISTINCT customer_id) AS unique_customers
    FROM {{ ref('stg_orders') }}
    GROUP BY 1
)

SELECT
      r.restaurant_id
    , r.restaurant_name
    , r.cuisine_type
    , r.city
    , r.is_partner
    , r.rating
    , o.total_orders
    , o.delivered_orders
    , o.cancelled_orders
    , o.cancelled_by_restaurant
    , o.fulfillment_rate
    , o.avg_prep_time_mins
    , o.avg_total_delivery_mins
    , o.sla_breached_orders
    , o.sla_breach_rate
    , o.total_food_revenue
    , o.total_restaurant_fee_paid
    , o.unique_customers
FROM restaurants r
LEFT JOIN order_stats o ON r.restaurant_id = o.restaurant_id
;