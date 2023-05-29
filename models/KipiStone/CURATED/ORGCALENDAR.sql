{{ config(materialized="table") }}

with calendar as (select * from {{ source("STG", "ORGCALENDAR") }})
select *
from calendar
