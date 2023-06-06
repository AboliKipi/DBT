with cust as (
    select *, TRUE as IS_ACTIVE, current_date as LAST_UPDATED_DATE from {{ source("CRTD", "CUSTOMERDETAILS") }}
)
select * from cust;