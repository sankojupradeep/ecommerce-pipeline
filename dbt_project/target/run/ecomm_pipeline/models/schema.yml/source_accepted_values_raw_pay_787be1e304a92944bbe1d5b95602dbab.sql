select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        payment_status as value_field,
        count(*) as n_records

    from ECOMM_DB.raw.payments
    group by payment_status

)

select *
from all_values
where value_field not in (
    'SUCCESS','FAILED','PENDING','REFUNDED'
)



      
    ) dbt_internal_test