{% macro cast_utc(col) %}
    timezone('UTC', cast({{ col }} as timestamp))
{% endmacro %}