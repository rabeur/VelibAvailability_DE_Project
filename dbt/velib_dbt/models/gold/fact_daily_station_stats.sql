{{
    config(
        materialized         = 'incremental',
        unique_key           = ['station_id', 'snapshot_date'],
        incremental_strategy = 'delete+insert'
    )
}}

with hourly as (

    select * from {{ ref('fact_hourly_availability') }}

    {% if is_incremental() %}
        where snapshot_date >= (select max(snapshot_date) - interval '1 day' from {{ this }})
    {% endif %}

),

daily as (

    select
        station_id,
        snapshot_date,

        -- Coverage
        count(distinct snapshot_hour) as active_hours,
        sum(num_snapshots) as total_snapshots,

        -- Average availability
        round(avg(avg_bikes_available)::numeric, 2) as avg_bikes_available,
        round(avg(avg_docks_available)::numeric, 2) as avg_docks_available,
        round(avg(avg_occupancy_rate)::numeric, 2) as avg_occupancy_rate,
        round(avg(avg_availability_rate)::numeric, 2) as avg_availability_rate,
        round(avg(avg_service_rate)::numeric, 2) as avg_service_rate,

        -- Intra-day peaks
        max(avg_bikes_available) as peak_hourly_avg_bikes,
        min(avg_bikes_available) as trough_hourly_avg_bikes,
        max(avg_occupancy_rate) as peak_hourly_occupancy,

        -- Hour of the day with highest average occupancy
        (array_agg(snapshot_hour order by avg_occupancy_rate desc))[1] as peak_hour,

        -- Stress indicators (minutes)
        sum(minutes_empty) as total_minutes_empty,
        sum(minutes_full) as total_minutes_full,

        -- Stress as % of total observed snapshots
        round(
            sum(minutes_empty) * 100.0 / nullif(sum(num_snapshots), 0),
            2
        ) as pct_time_empty,
        round(
            sum(minutes_full) * 100.0 / nullif(sum(num_snapshots), 0),
            2
        ) as pct_time_full,

        -- Reliability
        round(avg(operational_pct)::numeric, 2) as daily_operational_pct

    from hourly
    group by station_id, snapshot_date

)

select * from daily
