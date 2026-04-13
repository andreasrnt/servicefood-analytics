{{ config(materialized = 'table') }}

WITH funnel AS (
    SELECT * FROM {{ ref('fact_funnel') }}
)

, orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

, events AS (
    SELECT * FROM {{ ref('stg_events') }}
    WHERE funnel_stage = 'CHECKOUT'
)

, funnel_agg AS (
    SELECT
          channel
        , campaign
        , SUM(s1_app_visit) AS total_visits
        , SUM(s7_delivered) AS total_conversions
        , ROUND(AVG(cvr_visit_to_browse), 4) AS avg_cvr_visit_browse
        , ROUND(AVG(cvr_browse_to_cart), 4) AS avg_cvr_browse_cart
        , ROUND(AVG(cvr_cart_to_checkout), 4) AS avg_cvr_cart_checkout
        , ROUND(AVG(cvr_end_to_end), 4) AS avg_cvr_end_to_end
        , ROUND(AVG(dropoff_visit_to_browse), 4) AS avg_dropoff_visit_browse
        , ROUND(AVG(dropoff_browse_to_cart), 4) AS avg_dropoff_browse_cart
        , ROUND(AVG(dropoff_cart_to_checkout), 4) AS avg_dropoff_cart_checkout
    FROM funnel
    GROUP BY 1, 2
)

, checkout_counts AS (
    SELECT
          channel
        , COALESCE(campaign, 'NONE') AS campaign
        , COALESCE(voucher_code, 'NO_VOUCHER') AS voucher_code
        , COUNT(DISTINCT session_id) AS checkout_sessions
    FROM events
    GROUP BY 1, 2, 3
)

, order_agg AS (
    SELECT
          channel
        , COALESCE(campaign, 'NONE') AS campaign
        , COALESCE(voucher_code, 'NO_VOUCHER') AS voucher_code
        , COUNT(*) AS total_orders
        , COUNT(DISTINCT customer_id) AS unique_customers
        , COUNTIF(is_first_order) AS first_orders
        , COUNTIF(NOT is_first_order) AS repeat_orders
        , ROUND(SUM(gmv), 0) AS total_gmv
        , ROUND(SUM(discount_amount), 0) AS total_discount
        , ROUND(SUM(net_revenue), 0) AS total_net_revenue
        , ROUND(AVG(gmv), 0) AS avg_order_value
        , ROUND(SAFE_DIVIDE(SUM(discount_amount), SUM(gmv)), 4) AS discount_rate
        , ROUND(SAFE_DIVIDE(SUM(net_revenue), COUNT(DISTINCT customer_id)), 0) AS net_revenue_per_customer
        , COUNTIF(is_sla_breach = TRUE) AS sla_breached_orders
        , ROUND(SAFE_DIVIDE(
            COUNTIF(is_sla_breach = TRUE),
            COUNTIF(order_status = 'delivered')
          ), 4) AS sla_breach_rate
        , ROUND(SAFE_DIVIDE(SUM(food_price), NULLIF(SUM(discount_amount), 0)), 4) AS first_order_ratio
    FROM orders
    GROUP BY 1, 2, 3
)

SELECT
      o.channel
    , o.campaign
    , o.voucher_code

    , f.total_visits
    , f.total_conversions
    , f.avg_cvr_visit_browse
    , f.avg_cvr_browse_cart
    , f.avg_cvr_cart_checkout
    , f.avg_cvr_end_to_end
    , f.avg_dropoff_visit_browse
    , f.avg_dropoff_browse_cart
    , f.avg_dropoff_cart_checkout

    , c.checkout_sessions
    , ROUND(SAFE_DIVIDE(o.total_orders, c.checkout_sessions), 4) AS checkout_to_order_cvr

    , o.total_orders
    , o.unique_customers
    , o.first_orders
    , o.repeat_orders
    , ROUND(SAFE_DIVIDE(o.first_orders, o.total_orders), 4) AS first_order_ratio
    , o.total_gmv
    , o.total_discount
    , o.total_net_revenue
    , o.avg_order_value
    , o.discount_rate
    , o.net_revenue_per_customer
    , ROUND(SAFE_DIVIDE(o.total_net_revenue, f.total_visits), 2) AS net_revenue_per_visit
    , ROUND(SAFE_DIVIDE(o.total_net_revenue, c.checkout_sessions), 2) AS net_revenue_per_checkout
    , o.sla_breached_orders
    , o.sla_breach_rate

FROM order_agg o
LEFT JOIN funnel_agg f
    ON o.channel = f.channel
    AND o.campaign = f.campaign
LEFT JOIN checkout_counts c
    ON o.channel = c.channel
    AND o.campaign = c.campaign
    AND o.voucher_code = c.voucher_code
;