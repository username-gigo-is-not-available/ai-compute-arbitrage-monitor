{% macro mb_to_gb(col) %}
    {{ col }} / 1024.0
{% endmacro %}