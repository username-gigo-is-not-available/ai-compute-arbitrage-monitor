{% macro extract_tariff_block_number(col) %}
        regexp_extract(
            array_reverse(split({{ col }}, ' '))[safe_offset(0)],
            r'(\d+)'
    )
{% endmacro %}