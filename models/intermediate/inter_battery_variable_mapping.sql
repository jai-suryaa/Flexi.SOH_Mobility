{{ config(
    materialized='incremental',
    tags=['experimental', 'SOH']
) }}

WITH source_data AS (
    SELECT
        batteryId AS device_id,
        TRY_CAST(timestamp AS TIMESTAMP) AS ts,
        TRY_CAST(SOC AS DOUBLE) AS soc,
        TRY_CAST(HSC_Low AS DOUBLE) AS current,
        COALESCE(NULLIF(TRY_CAST(FullCapacity AS DOUBLE), 0), 49) AS nominal_capacity,
        TRY_CAST(VehicleState AS INTEGER) AS battery_state,
        TRY_CAST(Vmin AS DOUBLE) AS v_min,
        TRY_CAST(Vmax AS DOUBLE) AS v_max,

        TRY_CAST(V1  AS DOUBLE) AS v1,
        TRY_CAST(V2  AS DOUBLE) AS v2,
        TRY_CAST(V3  AS DOUBLE) AS v3,
        TRY_CAST(V4  AS DOUBLE) AS v4,
        TRY_CAST(V5  AS DOUBLE) AS v5,
        TRY_CAST(V6  AS DOUBLE) AS v6,
        TRY_CAST(V7  AS DOUBLE) AS v7,
        TRY_CAST(V8  AS DOUBLE) AS v8,
        TRY_CAST(V9  AS DOUBLE) AS v9,
        TRY_CAST(V10 AS DOUBLE) AS v10,
        TRY_CAST(V11 AS DOUBLE) AS v11,
        TRY_CAST(V12 AS DOUBLE) AS v12,
        TRY_CAST(V13 AS DOUBLE) AS v13,
        TRY_CAST(V14 AS DOUBLE) AS v14,
        TRY_CAST(V15 AS DOUBLE) AS v15,
        TRY_CAST(V16 AS DOUBLE) AS v16,

        TRY_CAST(Temp6  AS DOUBLE) AS temp01,
        TRY_CAST(Temp1  AS DOUBLE) AS temp02,
        ((TRY_CAST(Temp1 AS DOUBLE) + TRY_CAST(Temp2 AS DOUBLE)) / 2.0) AS temp03,
        TRY_CAST(Temp2  AS DOUBLE) AS temp04,
        TRY_CAST(Temp5  AS DOUBLE) AS temp05,
        ((TRY_CAST(Temp5 AS DOUBLE) + TRY_CAST(Temp4 AS DOUBLE)) / 2.0) AS temp06,
        TRY_CAST(Temp4  AS DOUBLE) AS temp07,
        TRY_CAST(Temp3  AS DOUBLE) AS temp08,
        TRY_CAST(Temp11 AS DOUBLE) AS temp09,
        TRY_CAST(Temp12 AS DOUBLE) AS temp10,
        TRY_CAST(Temp10 AS DOUBLE) AS temp11,
        TRY_CAST(Temp13 AS DOUBLE) AS temp12,
        TRY_CAST(Temp9  AS DOUBLE) AS temp13,
        ((TRY_CAST(Temp8 AS DOUBLE) + TRY_CAST(Temp9 AS DOUBLE)) / 2.0) AS temp14,
        TRY_CAST(Temp8  AS DOUBLE) AS temp15,
        TRY_CAST(Temp14 AS DOUBLE) AS temp16

    FROM {{ ref('stg_battery_raw_parquet_data') }}
    WHERE batteryId IN ('2233786558040370260', '4107284003026496596', '1585268215994051668')
),

with_avg_temp AS (
    SELECT *,
        list_avg([
            temp01, temp02, temp03, temp04,
            temp05, temp06, temp07, temp08,
            temp09, temp10, temp11, temp12,
            temp13, temp14, temp15, temp16
        ]) AS avg_temp
    FROM source_data
),

time_diff AS (
    SELECT
        *,
        EXTRACT(EPOCH FROM (ts - LAG(ts, 1, ts) OVER (PARTITION BY device_id ORDER BY ts))) AS delta_t_seconds
    FROM with_avg_temp
),

q_calc AS (
    SELECT
        *,
        TRY_CAST(ABS(current) * (delta_t_seconds / 3600.0) AS DOUBLE) AS q_delta
    FROM time_diff
),

q_throughput_calc AS (
    SELECT
        *,
        TRY_CAST(SUM(q_delta) OVER (
            PARTITION BY device_id, DATE_TRUNC('day', ts)
            ORDER BY ts
        ) AS DOUBLE) AS q_throughput
    FROM q_calc
)

SELECT
    device_id,
    ts,
    soc,
    current,
    nominal_capacity,
    battery_state,
    v_min,
    v_max,
    v1, v2, v3, v4, v5, v6, v7, v8,
    v9, v10, v11, v12, v13, v14, v15, v16,

    ROUND(temp01, 2) AS temp01,
    ROUND(temp02, 2) AS temp02,
    ROUND(temp03, 2) AS temp03,
    ROUND(temp04, 2) AS temp04,
    ROUND(temp05, 2) AS temp05,
    ROUND(temp06, 2) AS temp06,
    ROUND(temp07, 2) AS temp07,
    ROUND(temp08, 2) AS temp08,
    ROUND(temp09, 2) AS temp09,
    ROUND(temp10, 2) AS temp10,
    ROUND(temp11, 2) AS temp11,
    ROUND(temp12, 2) AS temp12,
    ROUND(temp13, 2) AS temp13,
    ROUND(temp14, 2) AS temp14,
    ROUND(temp15, 2) AS temp15,
    ROUND(temp16, 2) AS temp16,

    ROUND(avg_temp, 2) AS avg_temp,

    TRY_CAST(q_delta      AS DOUBLE) AS q_delta,
    TRY_CAST(q_throughput AS DOUBLE) AS q_throughput

FROM q_throughput_calc

WHERE
    device_id IS NOT NULL
    AND ts IS NOT NULL
    AND current IS NOT NULL
    AND soc IS NOT NULL
    AND q_delta IS NOT NULL
    AND q_throughput IS NOT NULL

{% if is_incremental() %}
    AND ts > (SELECT MAX(ts) FROM {{ this }})
{% endif %}