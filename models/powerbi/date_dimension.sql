WITH time_cte AS (

select 
date(to_timestamp(replace(starttime,'"',''))) as trip_date,
hour(to_timestamp(replace(starttime,'"',''))) as trip_hour,
CASE 
    WHEN DAYOFWEEK(to_timestamp(replace(starttime,'"',''))) in (6,7) THEN 'WEEKEND'
    ELSE 'BUSINESSDAYS' 
    end as trip_day_type,
CASE
    WHEN MONTH(to_timestamp(replace(starttime,'"',''))) in (12,1,2) THEN 'WINTER'
    WHEN MONTH(to_timestamp(replace(starttime,'"',''))) in (3,4,5) THEN 'SPRING'
    WHEN MONTH(to_timestamp(replace(starttime,'"',''))) in (6,7,8) THEN 'SUMMER'
    ELSE 'AUTUMN'
    end as trip_period_station
from
{{ source('demo', 'bike') }}

)

select * from time_cte