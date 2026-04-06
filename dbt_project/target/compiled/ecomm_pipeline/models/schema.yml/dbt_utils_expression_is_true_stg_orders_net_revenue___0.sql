



select
    1
from ECOMM_DB.staging_staging.stg_orders

where not(net_revenue >= 0)

