with stations as (

    select * from {{ ref('stg_stations') }}

),

enriched as (

    select
        station_id,
        station_name,
        capacity,

        -- Capacity category for grouping in reports
        case
            when capacity <= 10 then 'small'
            when capacity <= 20 then 'medium'
            when capacity <= 35 then 'large'
            else 'extra_large'
        end                                             as capacity_category,

        latitude,
        longitude,
        district_municipality_names,
        insee_municipality_code,

        -- Normalised city label (most stations are in Paris or suburbs)
        case
            when district_municipality_names ilike '%paris%' then 'Paris'
            else district_municipality_names
        end                                             as city,

        first_seen_at,
        last_seen_at,
        is_active,

        current_timestamp                               as dbt_updated_at

    from stations

)

select * from enriched
