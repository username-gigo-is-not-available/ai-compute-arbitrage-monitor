select *
from {{ ref('stg_electricity_tariffs_schedule') }}
where start_hour >= end_hour