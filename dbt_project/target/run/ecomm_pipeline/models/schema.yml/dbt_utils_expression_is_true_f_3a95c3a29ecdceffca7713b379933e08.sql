select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from ECOMM_DB.staging_gold.fct_revenue

where not(payment_success_rate_pct between 0 and 100)


      
    ) dbt_internal_test