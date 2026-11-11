-- The fact has one row per machine and tariff tier; the summary counts each current machine once.

with summary as (
    select sum(number_of_machines) as machines
    from {{ ref('mart_gpu_model_summary') }}
),

fact as (
    select count(distinct machine_id) as machines
    from {{ ref('fct_compute_offers') }}
    where cast(valid_to as date) = date '9999-12-31'
)

select
    summary.machines as summary_machines,
    fact.machines as fact_machines
from summary
cross join fact
where summary.machines != fact.machines
