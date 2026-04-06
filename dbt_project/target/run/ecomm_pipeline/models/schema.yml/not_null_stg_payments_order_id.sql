select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select order_id
from ECOMM_DB.staging_staging.stg_payments
where order_id is null



      
    ) dbt_internal_test