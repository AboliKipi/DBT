{{ config(schema="REPORTING_SCHEMA") }}

with
    rating as (select * from {{ source("CRTD", "ratingmaster") }}),
    approach as (
        select
            *,
            case
                when sys_rating >= 4
                then 'Email'
                when sys_rating < 4 and sys_rating >= 3
                then 'Call'
                when sys_rating < 3
                then 'Call & Email'
            end as approach_method
        from rating
    ),
    cust as (
        select a.*, b.contact_no, b.email_id, b.RM
        from approach a
        join {{ source("CRTD", "CUSTOMER_MASTER") }} b on a.customerid = b.customer_id
    )
select * from cust