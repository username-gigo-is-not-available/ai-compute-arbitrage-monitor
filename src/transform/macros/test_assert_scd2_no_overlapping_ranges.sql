-- Two rows per natural key whose half-open [valid_from, valid_to) ranges overlap, active or not. Adjacent ranges
-- (one ends where the next starts) are how valid_to and the fct post-hook close a row, so they are not flagged.
-- Used from schema.yml as assert_scd2_no_overlapping_ranges: dbt drops the test_ prefix.
{% macro test_assert_scd2_no_overlapping_ranges(
    model,
    natural_key_columns,
    valid_from_column='valid_from',
    valid_to_column='valid_to'
) %}

    {%- set nk_partition = natural_key_columns | join(', ') -%}

with scd_rows as (
    select
        row_number() over (
            partition by {{ nk_partition }}
            order by {{ valid_from_column }}, {{ valid_to_column }}
        ) as scd_rn,
        t.*
    from {{ model }} as t
)

select
    {%- for col in natural_key_columns %}
    a.{{ col }},
    {%- endfor %}
    a.{{ valid_from_column }} as a_valid_from,
    a.{{ valid_to_column }} as a_valid_to,
    b.{{ valid_from_column }} as b_valid_from,
    b.{{ valid_to_column }} as b_valid_to
from scd_rows as a
join scd_rows as b
    on a.scd_rn < b.scd_rn
    {%- for col in natural_key_columns %}
    and a.{{ col }} is not distinct from b.{{ col }}
    {%- endfor %}
    and a.{{ valid_from_column }} < b.{{ valid_to_column }}
    and b.{{ valid_from_column }} < a.{{ valid_to_column }}
order by
{%- for col in natural_key_columns %}
    a.{{ col }},
{%- endfor %}
    a.scd_rn,
    b.scd_rn

{% endmacro %}
