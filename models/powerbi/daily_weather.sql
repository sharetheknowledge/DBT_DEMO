-- with daily_weather as (

-- select
-- date(time) as weather_day,


-- from {{ source('demo', 'weather') }}
-- group by weather_day

-- limit 10)

-- select * from daily_weather

with daily_weather as (

    select 
    date(time) as weather_day,
    weather,
    round(avg(clouds),2) as avg_clouds,
    round(avg(humidity),2) as avg_humidity,
    round(avg(pressure),2) as avg_pressure
    -- row_number() over(partition by date(time) order by count(weather) desc)
    from {{ source('demo', 'weather') }}

    group by date(time), weather

    qualify row_number() over(partition by date(time) order by count(weather) desc) =1
)


select * from daily_weather