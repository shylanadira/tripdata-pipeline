{{ config(materialized='table') }}

WITH trip_base AS (
    SELECT *
    FROM {{ ref('stg_trip_data') }}
),

payment_lookup AS (
    SELECT
        payment_type,
        description
    FROM `{{ project }}.{{ target.schema }}.payment_type`
)

-- Final output: transformed version of trip_base
SELECT
    t.lpep_pickup_datetime,
    t.lpep_dropoff_datetime,
    t.trip_duration_min,
    t.trip_distance,
    t.trip_distance_km,
    t.vendor_id,
    t.store_and_fwd_flag,
    t.ratecode_id,
    t.pu_location_id,
    t.do_location_id,
    t.passenger_count,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.ehail_fee,
    t.improvement_surcharge,
    t.total_amount,
    -- ⬇️ Overwrite payment_type with string label from lookup
    p.description AS payment_type,
    t.trip_type,
    t.congestion_surcharge
FROM trip_base t
LEFT JOIN payment_lookup p
    ON t.payment_type = p.payment_type