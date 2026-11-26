{#- Each EVN version is a full snapshot of its table, so a row ends where the TABLE's next version starts, not where
    its key's next row does: a key EVN drops would otherwise stay active forever (#143). -#}
{% macro with_snapshot_valid_to(relation) %}
    select
        rows_.*,
        coalesce(versions.next_valid_from, timestamp '9999-12-31') as valid_to
    from {{ relation }} as rows_
    inner join (
        select
            valid_from,
            lead(valid_from) over (order by valid_from) as next_valid_from
        from (select distinct valid_from from {{ relation }})
    ) as versions
        on rows_.valid_from = versions.valid_from
{% endmacro %}
