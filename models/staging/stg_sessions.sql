WITH source AS (
    SELECT * FROM {{ ref('sessions') }}
)

SELECT
      CAST(session_id AS INT64) AS session_id
    , CAST(user_id AS INT64) AS user_id
    , channel
    , NULLIF(campaign, '') AS campaign
    , device
    , city
    , CAST(session_start_at AS TIMESTAMP) AS session_start_at
    , DATE(CAST(session_start_at AS TIMESTAMP)) AS session_date
    , EXTRACT(DAYOFWEEK FROM CAST(session_start_at AS TIMESTAMP)) IN (1, 7) AS is_weekend
FROM source
;