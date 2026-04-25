with source as (

    select * from {{ source('silver', 'station_availability') }}

),

staged as (

    select
        station_id,
        snapshot_timestamp,
        {%- if target.type == 'bigquery' %}
        -- Postgres source has snapshot_date / snapshot_hour / snapshot_day_of_week
        -- as generated columns. BigQuery's silver.station_availability is
        -- written by the Spark connector without those derived fields, so we
        -- compute them here. Subtract 1 from DAYOFWEEK to match Postgres'
        -- 0-6 (Sunday=0) convention used downstream by Gold marts.
        date(snapshot_timestamp) as snapshot_date,
        extract(hour from snapshot_timestamp) as snapshot_hour,
        extract(dayofweek from snapshot_timestamp) - 1 as snapshot_day_of_week,
        {%- else %}
        snapshot_date,
        snapshot_hour,
        snapshot_day_of_week,
        {%- endif %}
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
    where
        station_id is not null
        and is_installed = true

)

select * from staged
