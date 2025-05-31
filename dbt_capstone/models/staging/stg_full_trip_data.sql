{{ config(
    materialized='incremental',
    unique_key='lpep_pickup_datetime'
) }}

WITH combined AS (
  SELECT * FROM {{ ref('stg_trip_data') }}
  UNION ALL
  SELECT * FROM {{ ref('stg_trip_data_stream') }}
)

SELECT * FROM combined

{% if is_incremental() %}
  WHERE lpep_pickup_datetime > (SELECT MAX(lpep_pickup_datetime) FROM {{ this }})
{% endif %}