select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from ECOMM_DB.staging_gold.fct_revenue

where not(aov >= 0)


      
    ) dbt_internal_test