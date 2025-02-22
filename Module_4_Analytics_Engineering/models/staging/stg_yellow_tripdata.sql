with

source as (

    select * from {{ source('staging', 'yellow_tripdata') }}

)

renamed as (

    select
        VendorID,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        store_and_fwd_flag,
        RatecodeID,
        PULocationID,
        DOLocationID,
        passenger_count,
        trip_distance,
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
)