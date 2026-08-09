{% macro extract_fee_type(label_col) %}
    case
        when {{ label_col }} = 'Активна електрична енергија' then 'distribution'
        when {{ label_col }} = 'Надоместок за пристап на електродистрибутивниот систем' then 'access'
    end
{% endmacro %}