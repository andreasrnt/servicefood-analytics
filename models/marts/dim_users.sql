{{ config(materialized = 'table') }}

WITH users AS (
    SELECT * FROM {{ ref('stg_users') }}
)

, orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    WHERE order_status = 'delivered'
)

, first_touch AS (
    SELECT
          customer_id
        , ARRAY_AGG(channel ORDER BY placed_at ASC LIMIT 1)[OFFSET(0)] AS first_channel
        , ARRAY_AGG(campaign ORDER BY placed_at ASC LIMIT 1)[OFFSET(0)] AS first_campaign
        , MIN(order_date) AS first_order_date
        , MAX(order_date) AS last_order_date
    FROM orders
    GROUP BY 1
)

, rfm_window AS (
    SELECT
          customer_id
        , COUNT(*) AS orders_in_90d
        , ROUND(SUM(gmv), 0) AS gmv_in_90d
    FROM orders
    WHERE order_date >= DATE_SUB(
        (SELECT MAX(order_date) FROM orders),
        INTERVAL 90 DAY
    )
    GROUP BY 1
)

, r_scores AS (
    SELECT
          o.customer_id
        , DATE_DIFF((SELECT MAX(order_date) FROM orders), MAX(o.order_date), DAY) AS recency_days
        , CASE
            WHEN DATE_DIFF((SELECT MAX(order_date) FROM orders), MAX(o.order_date), DAY) <= 30 THEN 3
            WHEN DATE_DIFF((SELECT MAX(order_date) FROM orders), MAX(o.order_date), DAY) <= 60 THEN 2
            ELSE 1
          END AS r_score
    FROM orders o
    GROUP BY 1
)

, rfm_scores AS (
    SELECT
          o.customer_id
        , COALESCE(r.orders_in_90d, 0) AS orders_in_90d
        , COALESCE(r.gmv_in_90d, 0) AS gmv_in_90d
        , recency_days
        , r_score
        , CASE
            WHEN COALESCE(r.orders_in_90d, 0) >= 5 THEN 3
            WHEN COALESCE(r.orders_in_90d, 0) >= 2 THEN 2
            ELSE 1
          END AS f_score
        , CASE
            WHEN PERCENT_RANK() OVER (ORDER BY COALESCE(r.gmv_in_90d, 0)) >= 0.70 THEN 3
            WHEN PERCENT_RANK() OVER (ORDER BY COALESCE(r.gmv_in_90d, 0)) >= 0.30 THEN 2
            ELSE 1
          END AS m_score
    FROM orders o
    LEFT JOIN rfm_window r 
      ON o.customer_id = r.customer_id
    LEFT JOIN r_scores rs
      ON o.customer_id = rs.customer_id
)

, segmented AS (
    SELECT
          *
        , r_score + f_score + m_score AS rfm_total
        , CASE
            WHEN r_score = 3 AND f_score = 3 AND m_score = 3 THEN 'VIP'
            WHEN (r_score + f_score + m_score) >= 7 THEN 'REGULAR'
            WHEN (r_score + f_score + m_score) >= 5 THEN 'OCCASIONAL'
            ELSE 'AT_RISK'
          END AS customer_segment
    FROM rfm_scores
)

SELECT
      u.customer_id
    , u.city
    , u.gender
    , u.registered_at
    , u.phone_number
    , COALESCE(s.customer_segment, 'AT_RISK') AS customer_segment
    , COALESCE(s.r_score, 1) AS r_score
    , COALESCE(s.f_score, 1) AS f_score
    , COALESCE(s.m_score, 1) AS m_score
    , COALESCE(s.rfm_total, 3) AS rfm_total
    , s.recency_days
    , ft.first_channel
    , ft.first_campaign
    , ft.first_order_date
    , ft.last_order_date
FROM users u
LEFT JOIN segmented s 
  ON u.customer_id = s.customer_id
LEFT JOIN first_touch ft 
  ON u.customer_id = ft.customer_id
;