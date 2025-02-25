{{
    config(
        materialized='table'
    )
}}

with trips_data as (
    select 
        service_type,
        pickup_quarter,
        pickup_year,
        pickup_year_quarter,
        total_amount
    from {{ ref('fact_trips') }}
    WHERE pickup_year in (2019, 2020)
),
revenue_data as (
    select
        -- groups
        service_type,
        pickup_quarter as revenue_quarter,
        pickup_year as revenue_year,
        pickup_year_quarter as revenue_year_quarter,
        sum(total_amount) as revenue_quarterly_total_amount
    from trips_data
    group by 1,2,3,4
)

select
    service_type,
    revenue_quarter,
    revenue_year,
    revenue_year_quarter,
    revenue_quarterly_total_amount,

    -- calculate yoy revenue growth using LAG
    ROUND(
    (revenue_quarterly_total_amount - 
        LAG(revenue_quarterly_total_amount)
        OVER (
            PARTITION BY service_type, revenue_quarter
            ORDER BY revenue_year
        )
    ) /
    NULLIF(LAG(revenue_quarterly_total_amount)
        OVER (
            PARTITION BY service_type, revenue_quarter
            ORDER BY revenue_year
        ), 0) * 100, 2
    ) AS yoy_revenue_growth

from revenue_data
ORDER BY service_type, revenue_year, revenue_quarter