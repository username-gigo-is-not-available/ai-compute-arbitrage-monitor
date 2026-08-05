{% macro extract_tariff_type(label_col) %}
    case
        when {{ label_col }} = 'Активна електрична енергија' then 'energy'
    end
{% endmacro %}