{% macro calculate_inverse(col) %}
    (1.0 / nullif({{ col }}, 0))
{% endmacro %}