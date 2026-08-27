{% macro expected_tariff_tier_count(as_of_expr) %}
    (
        select count(*)
        from {{ ref('dim_electricity_tariff_tiers') }} as _tier
        where cast(_tier.valid_from as date) <= cast({{ as_of_expr }} as date)
          and cast(_tier.valid_to   as date) >  cast({{ as_of_expr }} as date)
    )
{% endmacro %}