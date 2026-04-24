WITH source AS (
    SELECT * FROM {{ ref('exchange_rates_usd') }}
)

SELECT
      CAST(month AS INT64) AS month
    , CAST(year AS INT64) AS year
    , period
    , lcy
    , CAST(usd_to_lcy AS FLOAT64) AS usd_to_lcy
    , CAST(lcy_to_usd AS FLOAT64) AS lcy_to_usd
    , source
FROM source
;