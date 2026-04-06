select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select aov
from ECOMM_DB.staging_gold.fct_revenue
where aov is null



      
    ) dbt_internal_test