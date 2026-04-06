{% macro translate_tariff_description(col) %}
    case
        when {{col}} = 'Домаќинства ДВОТАРИФНО мерење (висока тарифа) ВТ1'
        then 'household_high_tariff_block_1'
        when {{col}} = 'Домаќинства ДВОТАРИФНО мерење (висока тарифа) ВТ2'
        then 'household_high_tariff_block_2'
        when {{col}} = 'Домаќинства ДВОТАРИФНО мерење (висока тарифа) ВТ3'
        then 'household_high_tariff_block_3'
        when {{col}} = 'Домаќинства ДВОТАРИФНО мерење (висока тарифа) ВТ4'
        then 'household_high_tariff_block_4'
        when {{col}} = 'Домаќинства ДВОТАРИФНО мерење (ниска тарифа) НТ'
        then 'household_low_tariff_block'
        when {{col}} = 'мали потрошувачи (висока тарифа) ВТ'
        then 'business_high_tariff_block'
        when {{col}} = 'мали потрошувачи (ниска тарифа) НТ'
        then 'business_low_tariff_block'
        when {{col}} = 'Надоместок за пренос и дистрибуција'
        then 'distribution_fee'
        else 'unknown_tariff'
    end
{% endmacro %}