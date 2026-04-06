select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select total_gmv
from ECOMM_DB.staging_gold.fct_revenue
where total_gmv is null



      
    ) dbt_internal_test