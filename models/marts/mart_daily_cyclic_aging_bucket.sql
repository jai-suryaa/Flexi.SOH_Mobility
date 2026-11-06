{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['device_id', 'date'],
    tags=['experimental', 'SOH']
) }}

SELECT
    device_id,
    DATE_TRUNC('day', ts) AS date,
    MAX(nominal_capacity) AS nominal_capacity,
    SUM(q_delta) AS daily_q_throughput

FROM {{ ref('inter_battery_variable_mapping') }}

WHERE
    device_id IS NOT NULL
    AND ts IS NOT NULL
    AND q_delta IS NOT NULL

GROUP BY
    device_id,
    DATE_TRUNC('day', ts)