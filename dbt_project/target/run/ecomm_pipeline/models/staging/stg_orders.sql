
  create or replace   view ECOMM_DB.staging_staging.stg_orders
  
   as (
    -- models/staging/stg_orders.sql
-- Real-world transformations:
-- 1. RFM-ready fields for customer segmentation
-- 2. Order value buckets for ML datasets
-- 3. Time-based features for analytics
-- 4. Delivery performance scoring

with source as (
    select * from ECOMM_DB.raw.orders
),

cleaned as (
    select
        -- Keys
        order_id,
        customer_id,
        product_id,

        -- Financials
        cast(quantity as integer)                           as quantity,
        cast(unit_price as numeric(12,2))                   as unit_price,
        coalesce(cast(discount_pct as numeric(5,2)), 0)     as discount_pct,
        coalesce(cast(discount_amount as numeric(12,2)), 0) as discount_amount,
        cast(gmv as numeric(14,2))                          as gmv,
        cast(net_revenue as numeric(14,2))                  as net_revenue,

        -- Status
        upper(trim(order_status))            as order_status,
        coalesce(is_returned, false)         as is_returned,
        return_reason,

        -- Geography
        initcap(city)   as city,
        initcap(state)  as state,
        pincode,

        -- Dates
        cast(order_date as timestamp)        as order_date,
        date(order_date)                     as order_date_day,
        cast(delivery_date as date)          as delivery_date,
        cast(created_at as timestamp)        as created_at,

        -- Time features for ML
        dayofweek(cast(order_date as date))  as order_day_of_week,
        dayofmonth(cast(order_date as date)) as order_day_of_month,
        month(cast(order_date as date))      as order_month,
        quarter(cast(order_date as date))    as order_quarter,
        year(cast(order_date as date))       as order_year,
        hour(cast(order_date as timestamp))  as order_hour,

        case
            when dayofweek(cast(order_date as date)) in (0,6) then true
            else false
        end                                  as is_weekend_order,

        case
            when hour(cast(order_date as timestamp)) between 6  and 11 then 'Morning'
            when hour(cast(order_date as timestamp)) between 12 and 16 then 'Afternoon'
            when hour(cast(order_date as timestamp)) between 17 and 20 then 'Evening'
            else 'Night'
        end                                  as order_time_slot,

        -- Order value buckets (Power BI slicers / ML features)
        case
            when cast(gmv as numeric) < 500   then 'Low (<500)'
            when cast(gmv as numeric) < 2000  then 'Medium (500-2K)'
            when cast(gmv as numeric) < 10000 then 'High (2K-10K)'
            else 'Premium (10K+)'
        end                                  as order_value_bucket,

        -- Discount aggressiveness
        case
            when coalesce(cast(discount_pct as numeric), 0) = 0   then 'No discount'
            when coalesce(cast(discount_pct as numeric), 0) <= 10 then 'Low (1-10%)'
            when coalesce(cast(discount_pct as numeric), 0) <= 20 then 'Medium (11-20%)'
            else 'High (21%+)'
        end                                  as discount_tier,

        -- Delivery performance
        datediff('day',
            cast(order_date as date),
            coalesce(cast(delivery_date as date), current_date())
        )                                    as days_to_deliver,

        case
            when order_status = 'DELIVERED'
              and datediff('day', cast(order_date as date), cast(delivery_date as date)) <= 2
                then 'Express'
            when order_status = 'DELIVERED'
              and datediff('day', cast(order_date as date), cast(delivery_date as date)) <= 5
                then 'Standard'
            when order_status = 'DELIVERED' then 'Delayed'
            else 'Not delivered'
        end                                  as delivery_speed,

        -- RFM: recency proxy
        datediff('day', cast(order_date as date), current_date()) as days_since_order,

        -- Revenue quality flags
        case when order_status = 'CANCELLED'                   then true else false end as is_cancelled,
        case when order_status in ('DELIVERED','SHIPPED')      then true else false end as is_fulfilled,

        -- Estimated margin
        round(
            (cast(net_revenue as numeric) - cast(gmv as numeric) * 0.3)
            / nullif(cast(gmv as numeric), 0) * 100
        , 2)                                 as estimated_margin_pct

    from source
    where order_id is not null
      and order_date is not null
      and gmv >= 0
)

select * from cleaned
  );

