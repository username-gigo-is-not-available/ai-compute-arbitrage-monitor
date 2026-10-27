{# Hourly electricity cost in USD (ADR-021, ADR-025): kWh x (tariff + distribution fee), VAT on top, converted from
   MKD. The monthly access fee is left out: it is paid whether or not the GPU runs. Expects tariff_value,
   distribution_fee, vat_rate and usd_to_mkd_rate in scope. #}
{% macro cost_usd_per_hr(kwh_per_hr) %}
    ({{ kwh_per_hr }} * (tariff_value + coalesce(distribution_fee, 0)))
        * (1 + vat_rate) / nullif(usd_to_mkd_rate, 0)
{% endmacro %}
