select
*
from {{ source('demo', 'weather') }}

limit 10