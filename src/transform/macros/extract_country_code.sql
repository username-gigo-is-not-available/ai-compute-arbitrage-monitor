{% macro extract_country_code(col) %}
    nullif(trim(split_part({{ col }}, ',', 2)), '')
{% endmacro %}