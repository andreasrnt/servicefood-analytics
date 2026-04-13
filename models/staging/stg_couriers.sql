WITH source AS (
    SELECT * FROM {{ ref('couriers') }}
)

SELECT
      CAST(courier_id AS INT64) AS courier_id
    , city
    , vehicle_type
    , CAST(joined_at AS TIMESTAMP) AS joined_at
    , courier_tier
    , CAST(rating AS FLOAT64) AS rating
FROM source
;