select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select return_rate_pct
from ECOMM_DB.staging_gold.fct_revenue
where return_rate_pct is null



      
    ) dbt_internal_test