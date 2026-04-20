{% macro extract_country_code(col) %}
    nullif(trim(split({{ col }}, ',')[safe_offset(1)]), '')
{% endmacro %}