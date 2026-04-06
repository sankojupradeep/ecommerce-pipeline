-- models/staging/stg_payments.sql
-- Real-world transformations:
-- 1. Payment funnel analysis fields
-- 2. Gateway performance tracking
-- 3. Refund analytics
-- 4. Power BI ready payment method grouping

with source as (
    select * from ECOMM_DB.raw.payments
),

cleaned as (
    select
        -- Keys
        payment_id,
        order_id,

        -- Payment details
        upper(trim(payment_method))             as payment_method,
        upper(trim(payment_status))             as payment_status,
        cast(amount as numeric(14,2))           as amount,
        coalesce(currency, 'INR')               as currency,
        gateway,
        gateway_txn_id,
        failure_reason,
        coalesce(cast(refund_amount as numeric(14,2)), 0) as refund_amount,

        -- Dates
        cast(payment_date as timestamp)         as payment_date,
        date(payment_date)                      as payment_date_day,
        cast(created_at as timestamp)           as created_at,

        -- Status flags
        case when payment_status = 'SUCCESS'  then true else false end as is_successful,
        case when payment_status = 'REFUNDED' then true else false end as is_refunded,
        case when payment_status = 'FAILED'   then true else false end as is_failed,
        case when payment_status = 'PENDING'  then true else false end as is_pending,

        -- Payment method grouping (Power BI slicer)
        case
            when payment_method in ('UPI', 'Wallet')                        then 'Digital Wallet'
            when payment_method in ('Credit Card', 'Debit Card')            then 'Card'
            when payment_method = 'Net Banking'                             then 'Net Banking'
            when payment_method = 'COD'                                     then 'Cash on Delivery'
            else 'Other'
        end                                     as payment_method_group,

        -- Gateway performance flag
        case
            when payment_status = 'FAILED' and gateway in ('Razorpay','PayU') then true
            else false
        end                                     as is_gateway_failure,

        -- Refund rate flag
        case
            when refund_amount > 0 then true else false
        end                                     as has_refund,

        -- Amount buckets for analytics
        case
            when cast(amount as numeric) < 500   then 'Low'
            when cast(amount as numeric) < 5000  then 'Medium'
            when cast(amount as numeric) < 20000 then 'High'
            else 'Premium'
        end                                     as amount_bucket,

        -- Time features
        hour(cast(payment_date as timestamp))   as payment_hour,
        dayofweek(cast(payment_date as date))   as payment_day_of_week,
        month(cast(payment_date as date))       as payment_month

    from source
    where payment_id is not null
)

select * from cleaned