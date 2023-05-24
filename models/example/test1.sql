{{config(
    materialized = "table"
)
}}

with cte_inv_age as (

    select *, (as_of_date-invoicedate) as INV_Age from prod_dwh.staging_schema.ardetails 
),

cte_inv_details as (
select a.*, 
b.week as INV_week ,
b.month as INV_MONTH,
b.quarter as INV_QUARTER,
b.year as INV_YEAR
from cte_inv_age a left join prod_dwh.staging_schema.orgcalender b on invoicedate = d_date
),

cte_partial_paid as(
select *, IFF(totalar<transactionamount, 'Yes', 'No') as Partial_Paid from cte_inv_details
)
,
--select * from cte_partial_paid;

cte_term_days as (
select pd.*,ct.term_days from cte_partial_paid pd left join PROD_DWH.CURATED_SCHEMA.CUSTOMER_TERMS ct on pd.customerid=ct.customer_id
),

cte_due_date as (
select td.*, (term_days-inv_age) as Due_days from cte_term_days td
),

cte_due_date_calc as (
select a.*, 
b.week as DUE_week ,
b.month as DUE_MONTH,
b.quarter as DUE_QUARTER,
b.year as DUE_YEAR
from cte_due_date a left join prod_dwh.staging_schema.orgcalender b on duedate = d_date
),

cte_due_bucket as(
select *, CASE  WHEN due_days < 0 then 'CURRENT' 
                WHEN due_days between 1 and 30 then '1-30 Days' 
                WHEN due_days between 31 and 60 then '31-60 Days'
                WHEN due_days between 61 and 90 then '61-90 Days'
                WHEN due_days between 91 and 180 then '91-180 Days'
                WHEN due_days between 181 and 360 then '181-360 Days'
                WHEN due_days > 360 then '360+ Days'
END as Due_Bucket
from cte_due_date_calc
),
final as (
    select cdb.*, cd.region, cd.sub_region, cd.RM from cte_due_bucket cdb left join PROD_DWH.CURATED_SCHEMA.CUSTOMER_MASTER cd
    on cdb.customerid = cd.customer_id
    where cd.is_active = TRUE

)
select * from final