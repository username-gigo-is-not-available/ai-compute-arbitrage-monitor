{% macro is_latest(timestamp_col, partition_cols) %}
    case
        when lead({{ timestamp_col }}) over (
                partition by {{ partition_cols }}
                order by {{ timestamp_col }}
            ) is null then true
        else false
    end
{% endmacro %}