-- mart_peak_hours_by_district
-- Aggregates average occupancy by (district × day-of-week × hour).
-- Answers: "In which district, on which day, at which hour are bikes hardest to find?"
-- day_of_week: 0 = Sunday, 1 = Monday … 6 = Saturday (PostgreSQL DOW convention)

with hourly as (

    select * from {{ ref('fact_hourly_availability') }}

),

stations as (

    select
        station_id,
        district_municipality_names,
        city,
        capacity_category
    from {{ ref('dim_stations') }}

),

joined as (

    select
        s.district_municipality_names,
        s.city,
        h.snapshot_day_of_week,
        h.snapshot_hour,

        count(distinct h.station_id) as num_stations,
        sum(h.num_snapshots) as total_snapshots,

        -- Average rates across all stations in the district
        round({{ numeric_cast('avg(h.avg_occupancy_rate)') }}, 2) as avg_occupancy_rate,
        round({{ numeric_cast('avg(h.avg_availability_rate)') }}, 2) as avg_availability_rate,
        round({{ numeric_cast('avg(h.avg_bikes_available)') }}, 2) as avg_bikes_available,
        round({{ numeric_cast('avg(h.avg_docks_available)') }}, 2) as avg_docks_available,

        -- % of station-minutes where stations were empty / full
        round(
            sum(h.minutes_empty) * 100.0 / nullif(sum(h.num_snapshots), 0),
            2
        ) as pct_station_minutes_empty,
        round(
            sum(h.minutes_full) * 100.0 / nullif(sum(h.num_snapshots), 0),
            2
        ) as pct_station_minutes_full

    from hourly as h
    inner join stations as s on h.station_id = s.station_id
    group by
        s.district_municipality_names,
        s.city,
        h.snapshot_day_of_week,
        h.snapshot_hour

),

ranked as (

    select
        *,
        -- Rank hours within each (district, day) combination
        rank() over (
            partition by district_municipality_names, snapshot_day_of_week
            order by avg_occupancy_rate desc
        ) as peak_hour_rank

    from joined

)

select * from ranked
