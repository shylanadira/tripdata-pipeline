{{ config(
    materialized='incremental',
    unique_key='lpep_pickup_datetime'
) }}

WITH source_data AS (
  SELECT * FROM {{ ref('stg_full_trip_data') }}
)

SELECT
  lower(cast(vendorid as string)) as vendor_id,
  lpep_pickup_datetime,
  lpep_dropoff_datetime,
  store_and_fwd_flag,
  ratecodeid,
  pulocationid,
  dolocationid,
  passenger_count,
  trip_distance * 1.60934 as trip_distance_km,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  ehail_fee,
  improvement_surcharge,
  total_amount,
  trip_type,
  congestion_surcharge,
  -- transformation
  DATETIME_DIFF(lpep_dropoff_datetime, lpep_pickup_datetime, MINUTE) as trip_duration_min,
  payment.description as payment_type
FROM source_data
LEFT JOIN `purwadika.jcdeol3_capstone3_shyla.payment_type` as payment
  ON cast(source_data.payment_type as string) = cast(payment.payment_type as string)

{% if is_incremental() %}
  WHERE lpep_pickup_datetime > (SELECT MAX(lpep_pickup_datetime) FROM {{ this }})
{% endif %}