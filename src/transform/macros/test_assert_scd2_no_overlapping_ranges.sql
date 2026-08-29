-- Generic dbt test: assert_scd2_no_overlapping_ranges.
--
-- Flags SCD Type 2 "overlapping ranges" violations: any two rows sharing the same
-- natural key whose [valid_from, valid_to) intervals overlap, active or not.
--
-- Overlap uses half-open interval semantics, matching the pipeline's as-of joins
-- (`valid_from <= date < valid_to`) and the SQL standard's OVERLAPS operator:
-- a and b overlap iff a.valid_from < b.valid_to AND b.valid_from < a.valid_to.
-- Adjacent ranges (one ends exactly where the next starts) are legal by
-- construction (dims: valid_to = next valid_from - 1 day; fct: post_hook closes
-- with valid_to = next snapshot's valid_from), so they are NOT flagged.
--
-- Applied via schema.yml model-level tests by natural key, so a single macro
-- serves every SCD table (YAML key drops the test_ prefix, hence assert_*).
--
-- Failure output: the offending rows as the natural key plus BOTH (valid_from,
-- valid_to) pairs of each overlapping pair of rows.

{% macro test_assert_scd2_no_overlapping_ranges(model, natural_key_columns, valid_from_column='valid_from', valid_to_column='valid_to') %}

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
    a.{{ valid_to_column }}   as a_valid_to,
    b.{{ valid_from_column }} as b_valid_from,
    b.{{ valid_to_column }}   as b_valid_to
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