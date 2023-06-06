--Test if invoice date is greater than payment date
with pay as 
(
    select 1 from {{ref('PAYMENT_MASTER')}} where invoicedate>paymentdate
)
select * from pay