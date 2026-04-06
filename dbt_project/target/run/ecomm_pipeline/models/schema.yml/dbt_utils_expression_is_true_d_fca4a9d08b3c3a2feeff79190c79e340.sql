select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from ECOMM_DB.staging_gold.dim_products

where not(margin_pct between -100 and 100)


      
    ) dbt_internal_test