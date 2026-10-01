{#- Half-open [valid_from, valid_to): a version ends exactly where the next one starts (ADR-011, #34). -#}
{% macro valid_to(timestamp_col, partition_cols) %}
    lead({{ timestamp_col }}) over (
        partition by {{ partition_cols }}
        order by {{ timestamp_col }}
    )
{% endmacro %}
