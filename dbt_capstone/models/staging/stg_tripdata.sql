{{ config(materialized='view') }}

-- Cleaned + standardized base trip data

SELECT
    -- Timestamps
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    TIMESTAMP_DIFF(lpep_dropoff_datetime, lpep_pickup_datetime, MINUTE) AS trip_duration_min,

    -- Distance
    trip_distance,
    trip_distance * 1.60934 AS trip_distance_km,

    -- Column renaming to snake_case
    VendorID AS vendor_id,
    store_and_fwd_flag,
    RatecodeID AS ratecode_id,
    PULocationID AS pu_location_id,
    DOLocationID AS do_location_id,
    passenger_count,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge

FROM `purwadika.jcdeol3_capstone3_shyla.trip_data`