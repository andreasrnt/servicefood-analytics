-- Returns rows when test FAILS
-- Checks two failure modes:
--   1. is_sla_breach = TRUE but total_delivery_mins <= threshold (false positive)
--   2. is_sla_breach = FALSE but total_delivery_mins > threshold (false negative)

SELECT
      order_id
    , total_delivery_mins
    , delivery_distance_km
    , ROUND(30 + (delivery_distance_km * 3), 1) AS sla_threshold_mins
    , is_sla_breach
    , 'false_positive' AS failure_type
FROM {{ ref('stg_orders') }}
WHERE order_status = 'delivered'
  AND total_delivery_mins IS NOT NULL
  AND is_sla_breach = TRUE
  AND total_delivery_mins <= ROUND(30 + (delivery_distance_km * 3), 1)

UNION ALL

SELECT
      order_id
    , total_delivery_mins
    , delivery_distance_km
    , ROUND(30 + (delivery_distance_km * 3), 1) AS sla_threshold_mins
    , is_sla_breach
    , 'false_negative' AS failure_type
FROM {{ ref('stg_orders') }}
WHERE order_status = 'delivered'
  AND total_delivery_mins IS NOT NULL
  AND is_sla_breach = FALSE
  AND total_delivery_mins > ROUND(30 + (delivery_distance_km * 3), 1)