--Test if any invoice date is greater than due date
with ar as 
(
    select 1 from {{ref('AR_MASTER')}} where invoicedate>duedate
)
select * from ar