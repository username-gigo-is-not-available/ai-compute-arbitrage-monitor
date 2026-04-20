{% macro extract_tariff_type(col) %}
    case
        when array_reverse(split({{ col }}, ' '))[safe_offset(0)] in ('ВТ1', 'ВТ2', 'ВТ3', 'ВТ4', 'ВТ')
        then 'high'
        when array_reverse(split({{ col }}, ' '))[safe_offset(0)] = 'НТ'
        then 'low'
        else null
    end
{% endmacro %}