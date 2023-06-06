{{ config(materialized="table") }}

with terms as (select * from {{ source("STG", "CUSTOMER_TERMS") }})
select *
from terms
