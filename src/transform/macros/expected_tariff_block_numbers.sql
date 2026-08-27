{% macro expected_tariff_block_numbers(as_of_expr, subject_alias='t') %}
    (
        select 1
        from {{ ref('dim_electricity_tariff_blocks') }} as _block
        where _block.consumer_category    = {{ subject_alias }}.consumer_category
          and _block.tariff_window_type   = {{ subject_alias }}.tariff_window_type
          and _block.tariff_block_number  = {{ subject_alias }}.tariff_block_number
          and cast(_block.valid_from as date) <= cast({{ as_of_expr }} as date)
          and cast(_block.valid_to   as date) >  cast({{ as_of_expr }} as date)
    )
{% endmacro %}