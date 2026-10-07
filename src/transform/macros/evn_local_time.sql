{# The EVN time-of-use schedule is in Macedonian local time; timestamps in the warehouse are UTC (ADR-021).
   Convert before matching a timestamp to a schedule day_of_week (1 = Monday ... 7 = Sunday) or hour. #}
{% macro evn_day_of_week(ts) %}
    mod(extract(dayofweek from {{ ts }} at time zone '{{ var("evn_timezone") }}') + 5, 7) + 1
{% endmacro %}

{% macro evn_hour(ts) %}
    extract(hour from {{ ts }} at time zone '{{ var("evn_timezone") }}')
{% endmacro %}
