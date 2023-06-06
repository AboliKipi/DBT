--Test for Negative or zero Transaction Amount
with amt as 
(
    select transactionamount from {{ref('PAYMENT_MASTER')}} where transactionamount<=0
)
select * from amt