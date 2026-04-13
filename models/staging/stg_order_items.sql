WITH source AS (
    SELECT * FROM {{ ref('order_items') }}
)

SELECT
      CAST(item_id AS INT64) AS item_id
    , CAST(order_id AS INT64) AS order_id
    , item_name
    , cuisine_type
    , CAST(unit_price AS FLOAT64) AS unit_price
    , CAST(quantity AS INT64) AS quantity
    , CAST(unit_price AS FLOAT64) * CAST(quantity AS INT64) AS line_total
FROM source
;