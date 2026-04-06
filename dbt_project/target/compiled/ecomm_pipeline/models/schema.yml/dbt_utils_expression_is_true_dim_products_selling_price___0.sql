



select
    1
from ECOMM_DB.staging_gold.dim_products

where not(selling_price > 0)

