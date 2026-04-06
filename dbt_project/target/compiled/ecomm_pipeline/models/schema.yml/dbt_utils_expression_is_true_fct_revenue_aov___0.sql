



select
    1
from ECOMM_DB.staging_gold.fct_revenue

where not(aov >= 0)

