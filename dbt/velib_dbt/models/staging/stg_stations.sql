with source as (

    select * from {{ source('silver', 'stations') }}

),

staged as (

    select
        station_id,
        station_name,
        capacity,
        latitude,
        longitude,
        district_municipality_names,
        insee_municipality_code,
        first_seen_at,
        last_seen_at,
        is_active
    from source
    where
        station_id is not null
        and capacity > 0
        and is_active = true

)

select * from staged
