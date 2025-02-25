{{ config(materialized="table") }}

with
    fhv_tripdata as (select *, from {{ ref("stg_fhv_tripdata") }}),
    dim_zones as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')

select
    fhv_tripdata.fhv_id,
    fhv_tripdata.dispatching_base_num,
    fhv_tripdata.affiliated_base_number,
    fhv_tripdata.pickup_locationid,
    fhv_tripdata.dropoff_locationid,
    fhv_tripdata.sr_flag,
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.zone as dropoff_zone,
    extract(month from pickup_datetime) as pickup_month,
    extract(year from pickup_datetime) as pickup_year
from fhv_tripdata
inner join
    dim_zones as pickup_zone
    on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join
    dim_zones as dropoff_zone
    on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid
