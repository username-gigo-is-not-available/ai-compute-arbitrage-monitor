{% macro expected_tariff_tier_count(as_of_expr) %}
    (
        select count(*)
        from {{ ref('dim_electricity_tariff_tiers') }} as _tier
        where _tier.valid_from <= {{ as_of_expr }}
          and _tier.valid_to   >  {{ as_of_expr }}
    )
{% endmacro %}
