{{ config(
    materialized='incremental',
    unique_key='order_id'
)}}

with src as (
    select
        order_id,
        customer_id,
        order_date,
        order_status,
        shipping_cost,
        payment_method,
        _extracted_at,
        _source,
        _batch_id
    from {{source('bronze_bq', 'orders_bronze')}}

    {% if is_incremental() %}
        where _extracted_at > (
            select max(_extracted_at) from {{ this }}
        )
    {% endif %}
),

cleaned as (
    select
        CAST(order_id as INT64) as order_id,
        CAST(customer_id as INT64) as customer_id,
        CAST(order_date as TIMESTAMP) as order_date,
        lower(trim(order_status)) as order_status,
        CAST(shipping_cost as NUMERIC) as shipping_cost,
        lower(payment_method) as payment_method,
        _extracted_at as _extracted_at,
        _source as _source,
        _batch_id as _batch_id
    from src
)

select * from cleaned