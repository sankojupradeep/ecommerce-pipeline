



select
    1
from ECOMM_DB.staging_staging.stg_orders

where not(gmv >= 0)

