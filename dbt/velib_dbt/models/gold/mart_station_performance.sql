-- mart_station_performance
-- One row per station — overall KPIs aggregated across the full history.
-- Useful for dashboards, ranking, and alerting on under-performing stations.

with daily as (

    select * from {{ ref('fact_daily_station_stats') }}

),

stations as (

    select * from {{ ref('dim_stations') }}

),

summary as (

    select
        d.station_id,
        s.station_name,
        s.district_municipality_names,
        s.city,
        s.capacity,
        s.capacity_category,
        s.latitude,
        s.longitude,

        -- Observation window
        count(distinct d.snapshot_date) as days_observed,
        sum(d.total_snapshots) as total_snapshots,
        min(d.snapshot_date) as first_data_date,
        max(d.snapshot_date) as last_data_date,

        -- Central availability metrics
        round({{ numeric_cast('avg(d.avg_occupancy_rate)') }}, 2) as avg_occupancy_rate,
        round({{ numeric_cast('avg(d.avg_service_rate)') }}, 2) as avg_service_rate,
        round({{ numeric_cast('avg(d.avg_bikes_available)') }}, 2) as avg_bikes_available,
        round({{ numeric_cast('avg(d.avg_docks_available)') }}, 2) as avg_docks_available,

        -- Reliability
        round({{ numeric_cast('avg(d.daily_operational_pct)') }}, 2) as avg_operational_pct,

        -- Stress
        round({{ numeric_cast('avg(d.pct_time_empty)') }}, 2) as avg_pct_time_empty,
        round({{ numeric_cast('avg(d.pct_time_full)') }}, 2) as avg_pct_time_full,
        sum(d.total_minutes_empty) as total_minutes_empty,
        sum(d.total_minutes_full) as total_minutes_full,

        -- Peak behaviour
        round({{ numeric_cast('avg(d.peak_hourly_occupancy)') }}, 2) as avg_peak_hourly_occupancy,

        current_timestamp as dbt_updated_at

    from daily as d
    inner join stations as s on d.station_id = s.station_id
    group by
        d.station_id,
        s.station_name,
        s.district_municipality_names,
        s.city,
        s.capacity,
        s.capacity_category,
        s.latitude,
        s.longitude

),

ranked as (

    select
        *,

        -- Global rankings (1 = busiest / most stressed)
        rank() over (order by avg_occupancy_rate desc) as global_occupancy_rank,
        rank() over (order by avg_pct_time_empty desc) as most_empty_rank,
        rank() over (order by avg_pct_time_full desc) as most_full_rank,
        rank() over (order by avg_operational_pct asc) as least_reliable_rank,

        -- Within-district ranking
        rank() over (
            partition by district_municipality_names
            order by avg_occupancy_rate desc
        ) as district_occupancy_rank

    from summary

)

select * from ranked
