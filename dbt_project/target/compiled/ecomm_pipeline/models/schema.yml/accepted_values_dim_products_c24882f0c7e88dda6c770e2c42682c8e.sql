
    
    

with all_values as (

    select
        category as value_field,
        count(*) as n_records

    from ECOMM_DB.staging_gold.dim_products
    group by category

)

select *
from all_values
where value_field not in (
    'Electronics','Fashion','Home','Beauty','Sports','Books','Grocery'
)


