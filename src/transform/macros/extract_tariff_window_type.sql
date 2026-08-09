{% macro extract_tariff_window_type(col) %}
    case
        when array_reverse(split({{ col }}, ' '))[safe_offset(0)] in ('BT1', 'BT2', 'BT3', 'BT4', 'BT')
        then 'high'
        when array_reverse(split({{ col }}, ' '))[safe_offset(0)] = 'HT'
        then 'low'
        else null
    end
{% endmacro %}