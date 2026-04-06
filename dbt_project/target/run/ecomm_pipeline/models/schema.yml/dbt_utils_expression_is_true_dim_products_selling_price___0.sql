select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from ECOMM_DB.staging_gold.dim_products

where not(selling_price > 0)


      
    ) dbt_internal_test