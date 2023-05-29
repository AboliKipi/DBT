{{ config(materialized="table") }}


with
    source_data as (select * from {{ ref("PAYMENT_MASTER") }}),

    customer_data as (select * from {{ source("CRTD", "CUSTOMER_MASTER") }}),

    dd as (
        select
            iff(inv_age > term_days, 1, 0) as delayed_pay,
            (inv_age - term_days) as delayed_time,
            inv_age,
            term_days,
            customerid,
            customername,
            age_weighted_amount,
            term_weighted_amount,
            transaction_amount_in_usd,
            before_due

        from source_data
    ),
    dd1 as (
        select
            sum(delayed_pay) as total_delayed_pays,
            avg(delayed_time) as avg_delayed_time,
            sum(transaction_amount_in_usd) as total_transaction_amnt_weightage,
            count(*) as total_invoice_pays,
            customerid
        from dd
        group by customerid
    ),

    rating as (
        select
            customerid,
            total_delayed_pays,
            total_invoice_pays,
            total_transaction_amnt_weightage,
            (
                (total_invoice_pays - total_delayed_pays) * 5 / total_invoice_pays
            ) as rating,
            avg_delayed_time,
            ((avg_delayed_time) * 5 / 270) as rating1
        from dd1
    ),

    final as (
        select
            r.customerid,
            r.rating,
            c.customer_name,
            c.region,
            c.sub_region,
            c.country,
            r.rating as sys_rating,
            case
                when r.rating between 0 and 2.5
                then 'Bad'
                when r.rating between 2.5 and 4
                then 'Average'
                when r.rating > 4
                then 'Good'
            end as rating_bucket
        from rating as r, customer_data as c
        where r.customerid = c.customer_id
    )

select *
from final
