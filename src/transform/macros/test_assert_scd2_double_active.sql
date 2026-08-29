-- Generic dbt test: assert_scd2_double_active.
--
-- Flags SCD Type 2 "double active" violations: more than one row sharing the same
-- natural key that is currently active, where "active" is defined by the table's
-- own sentinel literal (active_value), e.g. `date '9999-12-31'` for the five DATE
-- dims and `timestamp '9999-12-31'` for fct_compute_offers.
--
-- Applied via schema.yml model-level tests by natural key, so a single macro
-- serves every SCD table (YAML key drops the test_ prefix, hence assert_*).
--
-- Failure output: the offending rows as the natural key plus BOTH (valid_from,
-- valid_to) pairs of each offending pair of active rows, so the real shape of a
-- violation is visible in the run log.

{% macro test_assert_scd2_double_active(model, natural_key_columns, active_value, valid_from_column='valid_from', valid_to_column='valid_to') %}

{%- set nk_partition = natural_key_columns | join(', ') -%}

with scd_active as (
    select
        row_number() over (
            partition by {{ nk_partition }}
            order by {{ valid_from_column }}, {{ valid_to_column }}
        ) as scd_rn,
        t.*
    from {{ model }} as t
    where {{ valid_to_column }} = {{ active_value }}
)

select
    {%- for col in natural_key_columns %}
    a.{{ col }},
    {%- endfor %}
    a.{{ valid_from_column }} as a_valid_from,
    a.{{ valid_to_column }}   as a_valid_to,
    b.{{ valid_from_column }} as b_valid_from,
    b.{{ valid_to_column }}   as b_valid_to
from scd_active as a
join scd_active as b
    on a.scd_rn < b.scd_rn
    {%- for col in natural_key_columns %}
    and a.{{ col }} is not distinct from b.{{ col }}
    {%- endfor %}
order by
    {%- for col in natural_key_columns %}
    a.{{ col }},
    {%- endfor %}
    a.scd_rn,
    b.scd_rn

{% endmacro %}