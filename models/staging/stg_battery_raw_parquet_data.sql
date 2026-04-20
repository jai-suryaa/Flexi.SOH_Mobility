{{ config(materialized='view') }}

{% if not var('process_date', none) %}
    {% do exceptions.raise_compiler_error("process_date variable is not set.") %}
{% endif %}

{% set process_date = var('process_date') %}

{% set year = process_date.split('-')[0] %}
{% set month = process_date.split('-')[1] | int %}
{% set day = process_date.split('-')[2] | int %}

{{ log("S3 PATH: s3://tec-raw-data-archive/Year=" ~ year ~ "/Month=" ~ month ~ "/Day=" ~ day, info=True) }}

SELECT *
FROM read_parquet(
    's3://tec-raw-data-archive/Year={{ year }}/Month={{ month }}/Day={{ day }}/**/*.parquet',
    union_by_name=True
)
WHERE uniqueId != '__HIVE_DEFAULT_PARTITION__'