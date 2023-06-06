--Test for Negative AR
with ar as 
(
    select totalar from {{ref('AR_MASTER')}} where totalar<=0
)
select * from ar