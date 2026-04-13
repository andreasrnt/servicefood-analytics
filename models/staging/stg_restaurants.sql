WITH source AS (
    SELECT * FROM {{ ref('restaurants') }}
)

SELECT
      CAST(restaurant_id AS INT64) AS restaurant_id
    , restaurant_name
    , cuisine_type
    , city
    , CAST(is_partner AS BOOL) AS is_partner
    , CAST(rating AS FLOAT64) AS rating
FROM source