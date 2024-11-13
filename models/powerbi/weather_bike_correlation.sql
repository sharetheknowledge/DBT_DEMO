with trip_data as (

select
trip_date,
avg(tripduration_min) as avg_trip_duration,
max(tripduration_min) as max_trip_duration_recorded,
count(*) as trip_counts


from {{ ref('trip_fact'	) }}
group by trip_date
)

select
t.*,
w.weather,
w.avg_clouds,
w.avg_humidity,
w.avg_pressure
from trip_data t
left join {{ ref('daily_weather') }} w on t.trip_date=w.weather_day
