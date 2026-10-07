{# Hourly electricity cost in USD (ADR-021): kWh x (tariff + distribution fee), plus, for average cost, the monthly
   access fee spread over 730 hours; VAT on top; converted from MKD. Pass access_fee for average cost, omit it for
   marginal cost. Expects tariff_value, distribution_fee, vat_rate and usd_to_mkd_rate in scope. #}
{% macro cost_usd_per_hr(kwh_per_hr, access_fee=none) %}
    ({{ kwh_per_hr }} * (tariff_value + coalesce(distribution_fee, 0))
        {%- if access_fee is not none %} + coalesce({{ access_fee }}, 0) / 730.0{% endif %})
        * (1 + vat_rate) / nullif(usd_to_mkd_rate, 0)
{% endmacro %}
