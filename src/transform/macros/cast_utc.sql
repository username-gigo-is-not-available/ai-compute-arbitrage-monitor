{% macro cast_utc(col) %}
    cast({{ col }} as timestamp)
{% endmacro %}