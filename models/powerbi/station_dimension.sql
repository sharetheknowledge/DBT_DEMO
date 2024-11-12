with unique_stations as (
    select
    distinct
    start_station_id as station_id,
    start_station_name as station_name,
    start_station_latitude as station_latitude,
    start_station_longitude as station_longitude

    from {{ source('demo', 'bike') }}
)

select * from unique_stations 