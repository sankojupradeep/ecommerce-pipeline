
    
    

with all_values as (

    select
        payment_method as value_field,
        count(*) as n_records

    from ECOMM_DB.raw.payments
    group by payment_method

)

select *
from all_values
where value_field not in (
    'UPI','Credit Card','Debit Card','Net Banking','COD','Wallet'
)


