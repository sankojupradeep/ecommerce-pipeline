select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select gmv
from ECOMM_DB.raw.orders
where gmv is null



      
    ) dbt_internal_test