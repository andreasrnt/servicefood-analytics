{{ config(materialized = 'table') }}

WITH couriers AS (
    SELECT * FROM {{ ref('stg_couriers') }}
)

, order_stats AS (
    SELECT
          courier_id
        , COUNT(*) AS total_orders
        , COUNTIF(order_status = 'delivered') AS delivered_orders
        , COUNTIF(order_status != 'delivered') AS cancelled_orders
        , COUNTIF(is_cancelled_by_driver) AS cancelled_by_driver
        , ROUND(SAFE_DIVIDE(COUNTIF(order_status = 'delivered'), COUNT(*)), 4) AS completion_rate
        , ROUND(AVG(CASE WHEN order_status = 'delivered' THEN courier_ride_mins END), 1) AS avg_ride_mins
        , ROUND(AVG(CASE WHEN order_status = 'delivered' THEN courier_wait_mins END), 1) AS avg_wait_mins
        , ROUND(AVG(delivery_distance_km), 2) AS avg_distance_km
        , COUNTIF(is_sla_breach = TRUE) AS sla_breached_orders
        , ROUND(SAFE_DIVIDE(COUNTIF(is_sla_breach = TRUE), COUNTIF(order_status = 'delivered')), 4) AS sla_breach_rate
        , ROUND(SUM(CASE WHEN order_status = 'delivered' THEN courier_commission END), 0) AS total_commission_earned
        , COUNT(DISTINCT customer_id) AS unique_customers_served
    FROM {{ ref('stg_orders') }}
    GROUP BY 1
)

SELECT
      c.courier_id
    , c.city
    , c.vehicle_type
    , c.joined_at
    , c.courier_tier
    , c.rating AS courier_rating
    , o.total_orders
    , o.delivered_orders
    , o.cancelled_orders
    , o.cancelled_by_driver
    , o.completion_rate
    , o.avg_ride_mins
    , o.avg_wait_mins
    , o.avg_distance_km
    , o.sla_breached_orders
    , o.sla_breach_rate
    , o.total_commission_earned
    , o.unique_customers_served
    , CASE
        WHEN c.courier_tier = 'gold' AND o.sla_breach_rate >= 0.30 THEN 'UNDERPERFORMING'
        WHEN c.courier_tier = 'bronze' AND o.sla_breach_rate <= 0.10 THEN 'OVERPERFORMING'
        ELSE 'EXPECTED'
      END AS performance_vs_tier
FROM couriers c
LEFT JOIN order_stats o ON c.courier_id = o.courier_id
;