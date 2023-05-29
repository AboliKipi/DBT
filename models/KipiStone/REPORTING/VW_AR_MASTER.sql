{{ config(schema='REPORTING_SCHEMA') }}

with ffcte1 as (
select * from {{ref('AR_MASTER')}}
)
select * from ffcte1