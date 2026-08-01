select *
from {{ ref('stg_electricity_tariff_window_schedule') }}
where start_hour >= end_hour