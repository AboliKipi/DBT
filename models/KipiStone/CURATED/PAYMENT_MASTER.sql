{{ config(materialized="table") }}

with
    cte_inv_age as (

        select *, (paymentdate - invoicedate) as inv_age
        from {{ source("CRTD", "PAYMENT_DETAILS") }}
    ),

    cte_pay_details as (
        select
            a.*,
            b.week as pay_week,
            b.month as pay_month,
            b.quarter as pay_quarter,
            b.year as pay_year
        from cte_inv_age a
        left join {{ref('ORGCALENDAR')}} b on paymentdate = d_date
    ),

    cte_inv_details as (
        select
            a.*,
            b.week as inv_week,
            b.month as inv_month,
            b.quarter as inv_quarter,
            b.year as inv_year
        from cte_pay_details a
        left join {{ref('ORGCALENDAR')}} b on invoicedate = d_date
    ),

 -- Term days
    cte_term_days as (
        select pd.*, ct.term_days
        from cte_inv_details pd
        left join
            {{ref('CUSTOMER_TERMS')}} ct on pd.customerid = ct.customer_id
    ),

    cte_before_due as (
        select td.*, iff(inv_age <= term_days, 'Yes', 'No') as before_due
        from cte_term_days td
    ),
-- Adding customer info
    cust as (
        select cdb.*, cd.region, cd.sub_region, cd.country, cd.rm
        from cte_before_due cdb
        left join
            {{ source("CRTD", "CUSTOMER_MASTER") }} cd
            on cdb.customerid = cd.customer_id
        where cd.is_active = true
    ),
-- USD Conversion
    amount_usd as (
        select
            a.*,
            round(
                (a.transactionamount * b.conversion_rate), 2
            ) as transaction_amount_in_usd,
            round((a.paymentamount * b.conversion_rate), 2) as payment_amount_in_usd
        from cust a
        inner join
             {{ref('CONVERSIONRATE')}} b
            on a.transactioncurrency = b.transaction_curruncy
        where b.eom_date = (select max(eom_date) from conversionrate)
    ),
    weighted as (
        select
            au.*,
            round((payment_amount_in_usd * inv_age), 2) as age_weighted_amount,
            round((payment_amount_in_usd * term_days), 2) as term_weighted_amount
        from amount_usd au
    ),
    final as (select * from weighted)
select *
from final
