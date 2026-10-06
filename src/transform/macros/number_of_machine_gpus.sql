{#- GPUs in an offer's machine, rented or not; every offer of a machine gives the same answer (ADR-020). -#}
{% macro number_of_machine_gpus(offer_gpus='number_of_offer_gpus', fraction='gpu_fraction_of_machine') %}
    cast(round({{ offer_gpus }} / {{ fraction }}) as int64)
{% endmacro %}
