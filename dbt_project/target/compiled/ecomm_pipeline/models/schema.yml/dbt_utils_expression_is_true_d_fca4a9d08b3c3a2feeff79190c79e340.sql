



select
    1
from ECOMM_DB.staging_gold.dim_products

where not(margin_pct between -100 and 100)

