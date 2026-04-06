-- models/gold/fct_revenue.sql
-- Daily revenue fact table — the dashboard source of truth
-- Real-world transformations:
-- 1. GMV, AOV, return rate, payment success rate
-- 2. Revenue quality metrics for Power BI
-- 3. MoM and WoW growth (window functions)
-- 4. ML dataset: daily features for forecasting

{{
    config(
        materialized='incremental',
        unique_key='order_date',
        on_schema_change='fail',
        cluster_by=['order_date']
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
        where order_date_day >= dateadd(day, -3, (select max(order_date) from {{ this }}))
    {% endif %}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

products as (
    select product_id, category, price_tier, profitability_tier
    from {{ ref('dim_products') }}
),

-- Enrich orders with product and payment data
enriched as (
    select
        o.order_id,
        o.order_date_day,
        o.gmv,
        o.net_revenue,
        o.is_returned,
        o.is_cancelled,
        o.is_fulfilled,
        o.order_status,
        o.order_value_bucket,
        o.discount_pct,
        o.discount_amount,
        o.delivery_speed,
        o.days_to_deliver,
        o.is_weekend_order,
        o.order_time_slot,
        o.city,
        o.state,
        p.category,
        p.price_tier,
        p.profitability_tier,
        pay.payment_status,
        pay.payment_method_group,
        pay.is_successful          as payment_successful,
        pay.is_refunded            as payment_refunded,
        pay.refund_amount
    from orders o
    left join payments pay on o.order_id = pay.order_id
    left join products p   on o.product_id = p.product_id
),

-- Daily aggregation
daily as (
    select
        order_date_day                                                      as order_date,

        -- ── Volume metrics ─────────────────────────────────────────────────
        count(distinct order_id)                                            as total_orders,
        count(distinct case when is_fulfilled  then order_id end)          as fulfilled_orders,
        count(distinct case when is_cancelled  then order_id end)          as cancelled_orders,
        count(distinct case when is_returned   then order_id end)          as returned_orders,

        -- ── Revenue metrics ────────────────────────────────────────────────
        round(sum(gmv), 2)                                                  as total_gmv,
        round(sum(net_revenue), 2)                                          as total_net_revenue,
        round(sum(gmv) / nullif(count(distinct order_id), 0), 2)           as aov,
        round(sum(net_revenue) / nullif(count(distinct order_id), 0), 2)   as net_aov,

        -- ── Discount metrics ───────────────────────────────────────────────
        round(sum(discount_amount), 2)                                      as total_discount_given,
        round(avg(discount_pct), 2)                                         as avg_discount_pct,

        -- ── Return metrics ─────────────────────────────────────────────────
        round(count(case when is_returned then 1 end) * 100.0
            / nullif(count(distinct order_id), 0), 2)                      as return_rate_pct,
        round(sum(case when is_returned then gmv else 0 end), 2)           as returned_gmv,

        -- ── Payment metrics ────────────────────────────────────────────────
        round(count(case when payment_successful then 1 end) * 100.0
            / nullif(count(order_id), 0), 2)                               as payment_success_rate_pct,
        round(sum(coalesce(refund_amount, 0)), 2)                          as total_refund_amount,

        -- ── Delivery metrics ───────────────────────────────────────────────
        round(avg(case when days_to_deliver > 0 then days_to_deliver end), 1) as avg_delivery_days,
        count(case when delivery_speed = 'Express'  then 1 end)            as express_deliveries,
        count(case when delivery_speed = 'Delayed'  then 1 end)            as delayed_deliveries,

        -- ── Channel insights (Power BI breakdown) ─────────────────────────
        count(case when is_weekend_order then 1 end)                       as weekend_orders,
        count(case when order_time_slot = 'Morning'   then 1 end)          as morning_orders,
        count(case when order_time_slot = 'Afternoon' then 1 end)          as afternoon_orders,
        count(case when order_time_slot = 'Evening'   then 1 end)          as evening_orders,
        count(case when order_time_slot = 'Night'     then 1 end)          as night_orders,

        -- ── Payment method breakdown ───────────────────────────────────────
        count(case when payment_method_group = 'Digital Wallet'    then 1 end) as digital_wallet_orders,
        count(case when payment_method_group = 'Card'              then 1 end) as card_orders,
        count(case when payment_method_group = 'Cash on Delivery'  then 1 end) as cod_orders,

        -- ── Category performance ───────────────────────────────────────────
        count(case when category = 'Electronics' then 1 end)               as electronics_orders,
        count(case when category = 'Fashion'     then 1 end)               as fashion_orders,
        count(case when category = 'Grocery'     then 1 end)               as grocery_orders,

        -- ── Revenue quality score (0-100, higher = healthier) ─────────────
        round(
            (count(case when payment_successful then 1 end) * 100.0 / nullif(count(order_id), 0))
            * 0.4   -- 40% weight: payment success
            + (100 - count(case when is_returned then 1 end) * 100.0 / nullif(count(distinct order_id), 0))
            * 0.4   -- 40% weight: non-return rate
            + (100 - count(case when is_cancelled then 1 end) * 100.0 / nullif(count(distinct order_id), 0))
            * 0.2   -- 20% weight: non-cancellation rate
        , 1)                                                                as revenue_quality_score,

        -- Metadata
        current_timestamp()                                                 as dbt_updated_at

    from enriched
    group by order_date_day
),

-- ── Window functions for growth metrics (ML forecasting features) ──────────
final as (
    select
        *,

        -- WoW growth
        round(
            (total_gmv - lag(total_gmv, 7) over (order by order_date))
            / nullif(lag(total_gmv, 7) over (order by order_date), 0) * 100
        , 2)                                                                as gmv_wow_growth_pct,

        -- MoM growth (approx 30 days)
        round(
            (total_gmv - lag(total_gmv, 30) over (order by order_date))
            / nullif(lag(total_gmv, 30) over (order by order_date), 0) * 100
        , 2)                                                                as gmv_mom_growth_pct,

        -- 7-day rolling average GMV (smoothed trend for Power BI)
        round(
            avg(total_gmv) over (
                order by order_date
                rows between 6 preceding and current row
            )
        , 2)                                                                as gmv_7day_rolling_avg,

        -- Cumulative GMV (running total)
        round(
            sum(total_gmv) over (order by order_date)
        , 2)                                                                as gmv_cumulative

    from daily
)

select * from final