



select
    1
from ECOMM_DB.staging_staging.stg_payments

where not(amount >= 0)

