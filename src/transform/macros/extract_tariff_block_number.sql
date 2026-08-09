{% macro extract_tariff_block_number(col) %}
    cast(
        regexp_extract(
            array_reverse(split({{ col }}, ' '))[safe_offset(0)],
            r'(\d+)'
        ) as int64
    )
{% endmacro %}