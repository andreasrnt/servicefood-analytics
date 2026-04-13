WITH source AS (
    SELECT * FROM {{ ref('users') }}
)

SELECT
      CAST(customer_id AS INT64) AS customer_id
    , city
    , CAST(registered_at AS TIMESTAMP) AS registered_at
    , gender
    , phone_number
FROM source
;