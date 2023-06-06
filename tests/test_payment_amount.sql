--Test for Negative or Zero payment amount
with amt as 
(
    select 1 from {{ref('PAYMENT_MASTER')}} where paymentamount<=0
)
select * from amt