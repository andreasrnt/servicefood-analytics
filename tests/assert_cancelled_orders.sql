-- Returns rows when test FAILS
-- Cancelled or failed orders must not have non-zero financial values
-- courier_commission and platform_fee should also be 0 since no service was rendered

SELECT
      order_id
    , order_status
    , gmv
    , net_revenue
    , courier_commission
    , platform_fee
    , restaurant_fee
FROM {{ ref('stg_orders') }}
WHERE order_status IN ('cancelled', 'failed')
AND (
    COALESCE(gmv, 0) != 0
    OR COALESCE(net_revenue, 0) != 0
    OR COALESCE(courier_commission, 0) != 0
    OR COALESCE(platform_fee, 0) != 0
    OR COALESCE(restaurant_fee, 0) != 0
)