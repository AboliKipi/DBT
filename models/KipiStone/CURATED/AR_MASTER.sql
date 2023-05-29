{{ config(materialized="table") }}

with
    cte_inv_age as (

        select *, (as_of_date - invoicedate) as inv_age
        from {{ source("CRTD", "AR_DETAILS") }}
    ),

    cte_inv_details as (
        select
            a.*,
            b.week as inv_week,
            b.month as inv_month,
            b.quarter as inv_quarter,
            b.year as inv_year
        from cte_inv_age a
        left join {{ref('ORGCALENDAR')}} b on invoicedate = d_date
    ),

    cte_partial_paid as (
        select *, iff(totalar < transactionamount, 'Yes', 'No') as partial_paid
        from cte_inv_details
    ),
    -- select * from cte_partial_paid;
    cte_term_days as (
        select pd.*, ct.term_days
        from cte_partial_paid pd
        left join
            {{ref('CUSTOMER_TERMS')}} ct on pd.customerid = ct.customer_id
    ),

    cte_due_date as (
        select td.*, (inv_age - term_days) as due_days from cte_term_days td
    ),

    cte_due_date_calc as (
        select
            a.*,
            b.week as due_week,
            b.month as due_month,
            b.quarter as due_quarter,
            b.year as due_year
        from cte_due_date a
        left join {{ref('ORGCALENDAR')}} b on duedate = d_date
    ),

    cte_due_bucket as (
        select
            *,
            case
                when due_days <= 0
                then 'CURRENT'
                when due_days between 1 and 30
                then '1-30 Days'
                when due_days between 31 and 60
                then '31-60 Days'
                when due_days between 61 and 90
                then '61-90 Days'
                when due_days between 91 and 180
                then '91-180 Days'
                when due_days between 181 and 360
                then '181-360 Days'
                when due_days > 360
                then '360+ Days'
            end as due_bucket
        from cte_due_date_calc
    ),
    final as (
        select cdb.*, cd.region, cd.sub_region, cd.country, cd.rm
        from cte_due_bucket cdb
        left join
            {{ source("CRTD", "CUSTOMER_MASTER") }} cd
            on cdb.customerid = cd.customer_id
        where cd.is_active = true

    ),
    amount_usd as (
        select
            a.*,
            round(
                (a.transactionamount * b.conversion_rate), 2
            ) as transaction_amount_in_usd,
            round((a.totalar * b.conversion_rate), 2) as total_ar_in_usd
        from final a
        inner join
            {{ref('CONVERSIONRATE')}} b
            on a.transactioncurrency = b.transaction_curruncy
        where b.eom_date = (select max(eom_date) from conversionrate)
    )
select *
from amount_usd
