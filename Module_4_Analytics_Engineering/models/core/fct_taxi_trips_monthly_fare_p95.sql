{{
    config(
        materialized='table'
    )
}}

with trips_data as (
    select 
        service_type,
        fare_amount,
        trip_distance,
        payment_type_description,
        pickup_year,
        pickup_month
    from {{ ref('fact_trips') }}
    WHERE 
        fare_amount > 0
        AND trip_distance > 0
        AND lower(payment_type_description) in ('cash', 'credit card')
        AND pickup_year = 2020
        AND pickup_month = 4
),
percentiles as (
    select
        service_type,
        pickup_year as percentile_year,
        pickup_month as percentile_month,
        PERCENTILE_CONT(fare_amount, 0.97)
            OVER (PARTITION BY service_type, pickup_year, pickup_month) AS p97,
        PERCENTILE_CONT(fare_amount, 0.95)
            OVER (PARTITION BY service_type, pickup_year, pickup_month) AS p95,
        PERCENTILE_CONT(fare_amount, 0.90)
            OVER (PARTITION BY service_type, pickup_year, pickup_month) AS p90,
        ROW_NUMBER() OVER (PARTITION BY service_type, pickup_year, pickup_month ORDER BY fare_amount) as rn
    from trips_data
)
select
    service_type,
    percentile_year,
    percentile_month,
    p97,
    p95,
    p90
from percentiles
where rn = 1
order by service_type
