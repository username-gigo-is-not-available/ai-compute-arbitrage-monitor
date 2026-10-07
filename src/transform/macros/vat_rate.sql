{# VAT added on top of EVN's published prices (ADR-021). Households pay it; a VAT-registered business reclaims it,
   so its pre-VAT cost is already its real cost. #}
{% macro vat_rate(consumer_category) %}
    case when {{ consumer_category }} = 'household' then {{ var('household_vat_rate') }} else 0 end
{% endmacro %}
