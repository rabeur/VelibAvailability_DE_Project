with stations as (

    select * from {{ ref('stg_stations') }}

),

enriched as (

    select
        station_id,
        station_name,
        capacity,

        -- Capacity category for grouping in reports
        latitude,

        longitude,
        district_municipality_names,
        insee_municipality_code,
        first_seen_at,

        -- Normalised city label (most stations are in Paris or suburbs)
        last_seen_at,

        is_active,
        case
            when capacity <= 10 then 'small'
            when capacity <= 20 then 'medium'
            when capacity <= 35 then 'large'
            else 'extra_large'
        end as capacity_category,
        case
            -- BigQuery has no ILIKE; lower() + LIKE is equivalent and
            -- renders identically under Postgres.
            when lower(district_municipality_names) like '%paris%' then 'Paris'
            else district_municipality_names
        end as city,

        current_timestamp as dbt_updated_at

    from stations

)

select * from enriched
