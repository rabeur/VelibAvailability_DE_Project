with source as (

    select * from {{ source('silver', 'station_availability') }}

),

staged as (

    select
        station_id,
        snapshot_timestamp,
        snapshot_date,
        snapshot_hour,
        snapshot_day_of_week,
        num_bikes_available,
        num_bikes_available_mechanical,
        num_bikes_available_ebike,
        num_docks_available,
        is_installed,
        is_renting,
        is_returning,
        occupancy_rate,
        availability_rate,
        service_rate,
        is_empty,
        is_full,
        is_operational,
        ingestion_timestamp
    from source
    where station_id is not null
      and is_installed = true

)

select * from staged
