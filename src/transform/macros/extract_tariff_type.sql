{% macro extract_tariff_type(col) %}
    case
        when split_part({{ col }}, ' ', -1) in ('ВТ1', 'ВТ2', 'ВТ3', 'ВТ4', 'ВТ')
        then 'high'
        when split_part({{ col }}, ' ', -1) = 'НТ'
        then 'low'
        else null
    end
{% endmacro %}