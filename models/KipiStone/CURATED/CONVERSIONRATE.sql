{{ config(materialized="table") }}


with terms as (select * from {{ source("STG", "CONVERSIONRATE") }})
select *
from terms