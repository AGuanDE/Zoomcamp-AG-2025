{{ config(materialized='table') }}

select 
    locationid, 
    borough, 
    zone, 
    REPLACE(CAST(service_zone AS STRING), 'Boro', 'Green') AS service_zone
from {{ ref('taxi_zone_lookup') }}