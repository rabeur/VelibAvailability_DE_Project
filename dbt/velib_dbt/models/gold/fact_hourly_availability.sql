{{
    config(
        materialized      = 'incremental',
        unique_key        = ['station_id', 'snapshot_date', 'snapshot_hour'],
        incremental_strategy = 'delete+insert'
    )
}}

with availability as (

    select * from {{ ref('stg_station_availability') }}

    {% if is_incremental() %}
    -- On incremental runs: reprocess the last 2 days to absorb late-arriving data
        where snapshot_date >= (select {{ date_sub_days('max(snapshot_date)', 1) }} from {{ this }})
    {% endif %}

),

hourly_agg as (

    select
        station_id,
        snapshot_date,
        snapshot_hour,
        snapshot_day_of_week,

        -- Volume
        count(*) as num_snapshots,

        -- Bike counts
        round({{ numeric_cast('avg(num_bikes_available)') }}, 2) as avg_bikes_available,
        round({{ numeric_cast('avg(num_bikes_available_mechanical)') }}, 2) as avg_bikes_mechanical,
        round({{ numeric_cast('avg(num_bikes_available_ebike)') }}, 2) as avg_bikes_ebike,
        round({{ numeric_cast('avg(num_docks_available)') }}, 2) as avg_docks_available,

        -- Rates
        round({{ numeric_cast('avg(occupancy_rate)') }}, 2) as avg_occupancy_rate,
        round({{ numeric_cast('avg(availability_rate)') }}, 2) as avg_availability_rate,
        round({{ numeric_cast('avg(service_rate)') }}, 2) as avg_service_rate,

        -- Peak / trough within the hour
        max(num_bikes_available) as peak_bikes_available,
        min(num_bikes_available) as trough_bikes_available,

        -- Stress counters (one unit ≈ one minute)
        sum(case when is_empty then 1 else 0 end) as minutes_empty,
        sum(case when is_full then 1 else 0 end) as minutes_full,

        -- Reliability
        round(
            avg(case when is_operational then 1.0 else 0.0 end) * 100,
            2
        ) as operational_pct,

        -- Time boundaries of the measurements within this slot
        min(snapshot_timestamp) as hour_start,
        max(snapshot_timestamp) as hour_end

    from availability
    group by
        station_id,
        snapshot_date,
        snapshot_hour,
        snapshot_day_of_week

)

select * from hourly_agg
