{% macro round_gpu_mb(col) %}
(
    select t
    from unnest([
        2048, 3072, 4096, 5120, 6144, 8192, 10240, 11264, 12288,
        16384, 20480, 24576, 32768, 40960, 49152, 65536, 81920, 98304, 143360, 184320
    ]) as t
    order by abs(t - {{ col }})
    limit 1
)
{% endmacro %}