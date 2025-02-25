{{ config(materialized="table") }}

with
    fhv_data as (
        select *
        from {{ ref("dim_fhv_trips") }}
        where
            pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East')
            and pickup_month = 11
            and pickup_year = 2019
    ),
    trip_duration as (
        select
            pickup_year,
            pickup_month,
            pickup_zone,
            dropoff_zone,
            pickup_locationid,
            dropoff_locationid,
            timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_duration_secs
        from fhv_data
    ),
    route_percentiles as (
        select
            pickup_year,
            pickup_month,
            pickup_zone,
            dropoff_zone,
            pickup_locationid,
            dropoff_locationid,
            percentile_cont(trip_duration_secs, 0.90) over (
                partition by pickup_locationid, dropoff_locationid
            ) as p90
        from trip_duration
    ),
    distinct_routes as (
        select distinct
            pickup_year,
            pickup_month,
            pickup_zone,
            dropoff_zone,
            pickup_locationid,
            dropoff_locationid,
            p90
        from route_percentiles
    ),
    ranked_routes as (
        select
            pickup_year,
            pickup_month,
            pickup_zone,
            dropoff_zone,
            p90,
            rank() over (
                partition by pickup_zone
                order by p90 desc
            ) as rank
        from distinct_routes
    )
select
    pickup_year,
    pickup_month,
    pickup_zone,
    dropoff_zone,
    p90
from ranked_routes
where rank = 2
order by p90 desc