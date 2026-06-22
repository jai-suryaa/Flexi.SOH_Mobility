{{ config(materialized='view') }}

{% if not var('parquet_paths', none) %}
    {% do exceptions.raise_compiler_error("parquet_paths variable is not set.") %}
{% endif %}

{% set parquet_paths = var('parquet_paths') %}

{{ log("PARQUET FILE COUNT: " ~ (parquet_paths | length), info=True) }}

SELECT *
FROM read_parquet(
    {{ parquet_paths | tojson }},
    union_by_name = TRUE,
    hive_partitioning = TRUE
)