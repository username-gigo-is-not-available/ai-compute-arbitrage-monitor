{#- The fees table bills the same kWh as the tiers table, for the network that delivers it (distribution, with
    transmission built in), so its active-energy row is the distribution fee. -#}
{% macro extract_fee_type(label_col) %}
    case
        when {{ label_col }} = 'Активна електрична енергија' then 'distribution'
        when {{ label_col }} = 'Надоместок за пристап на електродистрибутивниот систем' then 'access'
    end
{% endmacro %}
