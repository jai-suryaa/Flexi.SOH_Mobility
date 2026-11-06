{{ config(
    materialized='incremental',
    tags=['experimental', 'SOH']
) }}

SELECT
    device_id,
    MAX(nominal_capacity) AS nominal_capacity,
    SUM(daily_q_throughput) AS total_q_throughput,
    '{{ var("process_date") }}'::date AS ts

FROM {{ ref('mart_daily_cyclic_aging_bucket') }}

WHERE
    date >= '{{ var("start_date") }}'::date
    AND date < '{{ var("end_date") }}'::date

GROUP BY
    device_id