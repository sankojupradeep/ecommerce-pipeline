-- models/gold/dim_products.sql
-- SCD Type 1: full refresh, latest record wins
-- Real-world transformations:
-- 1. Margin and profitability scoring
-- 2. Price tier segmentation for Power BI
-- 3. Inventory health flags for operations
-- 4. ML-ready product features

with source as (
    select * from {{ source('raw', 'products') }}
),

enriched as (
    select
        -- Keys
        product_id,
        product_name,
        category,
        sub_category,
        brand,

        -- Pricing
        cast(mrp as numeric(12,2))           as mrp,
        cast(selling_price as numeric(12,2)) as selling_price,
        cast(cost_price as numeric(12,2))    as cost_price,
        cast(stock_quantity as integer)       as stock_quantity,
        coalesce(is_active, true)             as is_active,
        cast(created_at as timestamp)         as created_at,
        cast(updated_at as timestamp)         as updated_at,

        -- Margin analysis
        round(
            (cast(selling_price as numeric) - cast(cost_price as numeric))
            / nullif(cast(selling_price as numeric), 0) * 100
        , 2)                                  as margin_pct,

        round(
            cast(selling_price as numeric) - cast(cost_price as numeric)
        , 2)                                  as margin_amount,

        -- MRP discount
        round(
            (cast(mrp as numeric) - cast(selling_price as numeric))
            / nullif(cast(mrp as numeric), 0) * 100
        , 2)                                  as discount_from_mrp_pct,

        -- Price tier (Power BI slicer)
        case
            when cast(selling_price as numeric) < 500   then 'Budget (<500)'
            when cast(selling_price as numeric) < 2000  then 'Economy (500-2K)'
            when cast(selling_price as numeric) < 10000 then 'Premium (2K-10K)'
            else 'Luxury (10K+)'
        end                                   as price_tier,

        -- Profitability tier (for analytics dashboards)
        case
            when round((cast(selling_price as numeric) - cast(cost_price as numeric))
                / nullif(cast(selling_price as numeric), 0) * 100, 2) >= 50 then 'High margin'
            when round((cast(selling_price as numeric) - cast(cost_price as numeric))
                / nullif(cast(selling_price as numeric), 0) * 100, 2) >= 25 then 'Medium margin'
            when round((cast(selling_price as numeric) - cast(cost_price as numeric))
                / nullif(cast(selling_price as numeric), 0) * 100, 2) >= 0  then 'Low margin'
            else 'Loss making'
        end                                   as profitability_tier,

        -- Inventory health (operations dashboard)
        case
            when cast(stock_quantity as integer) = 0    then 'Out of stock'
            when cast(stock_quantity as integer) < 10   then 'Critical (<10)'
            when cast(stock_quantity as integer) < 100  then 'Low (10-100)'
            when cast(stock_quantity as integer) < 1000 then 'Healthy (100-1K)'
            else 'Overstocked (1K+)'
        end                                   as inventory_health,

        -- Stock value (working capital metric)
        round(
            cast(cost_price as numeric) * cast(stock_quantity as integer)
        , 2)                                  as inventory_value,

        -- ML feature: days since last update
        datediff('day', cast(updated_at as date), current_date()) as days_since_update,

        current_timestamp()                   as dbt_loaded_at

    from source
    where product_id is not null
      and selling_price > 0
)

select * from enriched