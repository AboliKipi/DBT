{{ config(materialized="table") }}

with rating as (
select * from ratingmaster;

)
select * from rating