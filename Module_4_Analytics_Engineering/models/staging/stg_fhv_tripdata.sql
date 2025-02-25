with
    fhv_data as (
        select *
        from {{ source("staging", "fhv_tripdata") }}
        where dispatching_base_num is not null
    )
select
    -- identifiers
    {{
        dbt_utils.generate_surrogate_key(
            ["dispatching_base_num", "pickup_datetime", "dropOff_datetime"]
        )
    }} as fhv_id,
    {{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("STRING")) }}
    as dispatching_base_num,
    {{ dbt.safe_cast("Affiliated_base_number", api.Column.translate_type("STRING")) }}
    as affiliated_base_number,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }}
    as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }}
    as dropoff_locationid,
    {{ dbt.safe_cast("SR_Flag", api.Column.translate_type("STRING")) }} as sr_flag,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime
from fhv_data
