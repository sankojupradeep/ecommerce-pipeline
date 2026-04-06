
    
    

select
    order_date as unique_field,
    count(*) as n_records

from ECOMM_DB.staging_gold.fct_revenue
where order_date is not null
group by order_date
having count(*) > 1


