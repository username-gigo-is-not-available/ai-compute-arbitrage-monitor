{% macro valid_to(timestamp_col, partition_cols) %}
    lead({{ timestamp_col }}) over (
        partition by {{ partition_cols }}
        order by {{ timestamp_col }}
    ) - interval 1 day
{% endmacro %}