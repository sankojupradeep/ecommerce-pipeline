select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select customer_id
from ECOMM_DB.staging_staging.stg_orders
where customer_id is null



      
    ) dbt_internal_test