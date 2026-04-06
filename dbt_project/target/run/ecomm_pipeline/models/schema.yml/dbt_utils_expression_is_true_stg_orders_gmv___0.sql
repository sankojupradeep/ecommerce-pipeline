select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from ECOMM_DB.staging_staging.stg_orders

where not(gmv >= 0)


      
    ) dbt_internal_test