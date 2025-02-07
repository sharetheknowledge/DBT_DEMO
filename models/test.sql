
with cte as (
select
distinct(date(time)) as dateweather
from {{ source('demo', 'weather') }}


)

select dateweather from cte

order by dateweather desc