--Test for Negative or zero Transaction Amount
with ar as 
(
    select transactionamount from {{ref('AR_MASTER')}} where transactionamount<=0
)
select * from ar