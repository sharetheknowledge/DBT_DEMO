with trip_data as
(

    select
    date(to_timestamp(replace(starttime,'"',''))) as trip_date,
    round(try_to_number(tripduration)/60,2) as tripduration_min,
    try_to_number(start_station_id),
    try_to_number(end_station_id),
    try_to_number(bikeid) as bike_id,
    usertype as user_type,
    try_to_number(birthyear) as birth_year,
    try_to_number(gender),
    round(st_distance(st_makepoint(try_to_number(start_station_longitude),try_to_number(start_station_latitude)),
        st_makepoint(try_to_number(end_station_longitude), try_to_number(end_station_latitude)))) as distance_meters

    from {{ source('demo', 'bike') }}

),

trip_data_enriched as (

select 
t.*,
w.weather,
w.avg_clouds,
w.avg_humidity,
w.avg_pressure
from trip_data t
left join {{ ref('daily_weather') }} w on t.trip_date = w.weather_day
)

select
*
from trip_data_enriched


order by trip_date desc


