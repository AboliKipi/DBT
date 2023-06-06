{{ config(schema='REPORTING_SCHEMA') }}

with FINAL as (
select * from {{ref('PAYMENT_MASTER')}}
)
select * from FINAL