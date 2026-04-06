



select
    1
from ECOMM_DB.staging_gold.fct_revenue

where not(payment_success_rate_pct between 0 and 100)

