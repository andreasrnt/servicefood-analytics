WITH source AS (
    SELECT * FROM {{ ref('orders') }}
)

, typed AS (
    SELECT
          CAST(order_id AS INT64) AS order_id
        , CAST(session_id AS INT64) AS session_id
        , CAST(customer_id AS INT64) AS customer_id
        , CAST(restaurant_id AS INT64) AS restaurant_id
        , CAST(courier_id AS INT64) AS courier_id
        , channel
        , NULLIF(campaign, '') AS campaign
        , NULLIF(voucher_code, '') AS voucher_code
        , device
        , city
        , destination_name
        , CAST(delivery_distance_km AS FLOAT64) AS delivery_distance_km
        , order_type
        , CAST(placed_at AS TIMESTAMP) AS placed_at
        , CAST(NULLIF(food_ready_at, '') AS TIMESTAMP) AS food_ready_at
        , CAST(NULLIF(picked_up_at, '') AS TIMESTAMP) AS picked_up_at
        , CAST(NULLIF(delivered_at, '') AS TIMESTAMP) AS delivered_at
        , order_status
        , NULLIF(cancel_note, '') AS cancel_note
        , CAST(food_price AS FLOAT64) AS food_price
        , CAST(discount_amount AS FLOAT64) AS discount_amount
    FROM source
)

, derived AS (
    SELECT
          *
        , DATE(placed_at) AS order_date
        , EXTRACT(HOUR FROM placed_at) AS order_hour
        , EXTRACT(DAYOFWEEK FROM placed_at) IN (1, 7) AS is_weekend
        , (EXTRACT(HOUR FROM placed_at) BETWEEN 11 AND 13)
          OR (EXTRACT(HOUR FROM placed_at) BETWEEN 18 AND 21) AS is_peak_hour
        , CASE
            WHEN EXTRACT(HOUR FROM placed_at) BETWEEN 11 AND 13 THEN 'LUNCH'
            WHEN EXTRACT(HOUR FROM placed_at) BETWEEN 18 AND 21 THEN 'DINNER'
            WHEN EXTRACT(HOUR FROM placed_at) BETWEEN 7 AND 10 THEN 'BREAKFAST'
            ELSE 'OFF_PEAK'
          END AS time_slot
        , CASE order_type
            WHEN 'priority' THEN 'PRIORITY_DELIVERY'
            WHEN 'saver' THEN 'SAVER_DELIVERY'
            ELSE 'REGULAR_DELIVERY'
          END AS order_type_name
        , CASE order_type
            WHEN 'priority' THEN 7000
            WHEN 'saver' THEN -2000
            ELSE 0
          END AS order_type_adj
        , cancel_note IN ('driver_cancelled', 'driver_no_show') AS is_cancelled_by_driver
        , cancel_note IN ('restaurant_closed', 'restaurant_too_busy', 'out_of_stock') AS is_cancelled_by_restaurant
        , cancel_note = 'customer_cancelled' AS is_cancelled_by_customer
        , CASE
            WHEN order_status = 'delivered' AND food_ready_at IS NOT NULL
            THEN TIMESTAMP_DIFF(food_ready_at, placed_at, MINUTE)
          END AS prep_time_mins
        , CASE
            WHEN order_status = 'delivered' AND picked_up_at IS NOT NULL
            THEN TIMESTAMP_DIFF(picked_up_at, food_ready_at, MINUTE)
          END AS courier_wait_mins
        , CASE
            WHEN order_status = 'delivered' AND delivered_at IS NOT NULL
            THEN TIMESTAMP_DIFF(delivered_at, picked_up_at, MINUTE)
          END AS courier_ride_mins
        , CASE
            WHEN order_status = 'delivered' AND delivered_at IS NOT NULL
            THEN TIMESTAMP_DIFF(delivered_at, placed_at, MINUTE)
          END AS total_delivery_mins
        , ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY placed_at
          ) AS user_order_number
    FROM typed
)

, cost AS (
    SELECT
          *
        , GREATEST(15000, delivery_distance_km * 2000) AS base_delivery_price
    FROM derived
)

, final AS (
    SELECT
          *
        , base_delivery_price + order_type_adj AS delivery_price
        , ROUND(food_price * 0.01, 0) AS platform_fee
        , ROUND(food_price * 0.02, 0) AS restaurant_fee
        , ROUND((base_delivery_price + order_type_adj) * 0.80, 0) AS courier_commission
        , food_price + (base_delivery_price + order_type_adj) + ROUND(food_price * 0.01, 0) - discount_amount AS gmv
        , ROUND(food_price * 0.01, 0) + ROUND(food_price * 0.02, 0) + ROUND((base_delivery_price + order_type_adj) * 0.20, 0) AS net_revenue
        , CASE
            WHEN order_status != 'delivered' OR total_delivery_mins IS NULL THEN NULL
            ELSE total_delivery_mins > (30 + (delivery_distance_km * 3))
          END AS is_sla_breach
        , CASE
            WHEN order_status != 'delivered' OR total_delivery_mins IS NULL THEN NULL
            WHEN total_delivery_mins <= (30 + delivery_distance_km * 3) * 0.75 THEN 'FAST'
            WHEN total_delivery_mins <= (30 + delivery_distance_km * 3) THEN 'ON_TIME'
            WHEN total_delivery_mins <= (30 + delivery_distance_km * 3) * 1.25 THEN 'SLOW'
            ELSE 'VERY_SLOW'
          END AS delivery_bucket
        , CASE
            WHEN order_status != 'delivered' OR total_delivery_mins IS NULL THEN NULL
            WHEN prep_time_mins = GREATEST(prep_time_mins, courier_wait_mins, courier_ride_mins) THEN 'RESTAURANT_PREP'
            WHEN courier_wait_mins = GREATEST(prep_time_mins, courier_wait_mins, courier_ride_mins) THEN 'COURIER_WAIT'
            ELSE 'COURIER_RIDE'
          END AS dominant_bottleneck
        , (user_order_number = 1) AS is_first_order
    FROM cost
)

SELECT
      order_id
    , session_id
    , customer_id
    , restaurant_id
    , courier_id
    , channel
    , campaign
    , voucher_code
    , device
    , city
    , destination_name
    , delivery_distance_km
    , order_type
    , order_type_name
    , order_type_adj
    , placed_at
    , food_ready_at
    , picked_up_at
    , delivered_at
    , order_date
    , order_hour
    , is_weekend
    , is_peak_hour
    , time_slot
    , order_status
    , cancel_note
    , is_cancelled_by_driver
    , is_cancelled_by_restaurant
    , is_cancelled_by_customer
    , prep_time_mins
    , courier_wait_mins
    , courier_ride_mins
    , total_delivery_mins
    , is_sla_breach
    , delivery_bucket
    , dominant_bottleneck
    , food_price
    , discount_amount
    , base_delivery_price
    , delivery_price
    , platform_fee
    , restaurant_fee
    , courier_commission
    , gmv
    , net_revenue
    , user_order_number
    , is_first_order
FROM final
;