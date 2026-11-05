{{ config(materialized='view') }}

{% if not var('process_date', none) %}
    {% do exceptions.raise_compiler_error("process_date variable is not set.") %}
{% endif %}

{% set process_date = var('process_date') %}

{% set year  = process_date.split('-')[0] %}
{% set month = process_date.split('-')[1] | int %}
{% set day   = process_date.split('-')[2] | int %}

{{ log("Parquet Path: /home/tec-ubuntu/Pack-Parquets/Year=" ~ year ~ "/Month=" ~ month ~ "/Day=" ~ day, info=True) }}

SELECT *
FROM read_parquet(
    '/home/tec-ubuntu/Pack-Parquets/Year={{ year }}/Month={{ month }}/Day={{ day }}/uniqueId=*/*.parquet',
    union_by_name = TRUE,
    hive_partitioning = TRUE
)
WHERE uniqueId IN (
    '1297037835547110484',
    '3530823255018171476',
    '2089671369964252244',
    '9511603555870698580',
    '2017613775926389844',
    '1152922647471123540',
    '1152922647471189076',
    '648519489222274132',
    '648519489205627988',
    '3314650472904387668',
    '2017613775926324308',
    '1657325805736684628',
    '4323456785156533332',
    '1297037839842339924',
    '2522016934191688788',
    '3819053626874784852',
    '2233786558040108116',
    '1297037835547307092',
    '3746996032836856916',
    '2594074532525042772',
    '5332263101671402580',
    '1513210617677343828',
    '7061645358597794900',
    '360289113054178388',
    '936749865357405268',
    '2810247310343793748',
    '3026420092457577556',
    '504404301129772116',
    '720577083243818068',
    '360289113054047316',
    '1729383399774481492',
    '6557242200332299348',
    '1441153023622900820',
    '72058736902204500',
    '3602880844761001044',
    '72058741197565012'
)