{{ config(
    materialized='incremental',
    unique_key='order_items_id'
)}}

with src as (
    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount_amount,
        total_price,
        _extracted_at,
        _source,
        _batch_id
    from {{ source('bronze_bq', 'order_items_bronze') }}

    {% if is_incremental() %}
        where _extracted_at > (
            select max(_extracted_at) from {{ this }}
        )
    {% endif %}
),

cleaned as (
    select
        CAST(order_item_id as INT64) as order_item_id,
        CAST(order_id as INT64) as order_id,
        CAST(product_id as INT64) as product_id,
        CAST(quantity as INT64) as quantity,
        CAST(unit_price as NUMERIC) as unit_price,
        CAST(discount_amount as NUMERIC) as discount_amount,
        CAST(total_price as NUMERIC) as total_price,
        _extracted_at as _extracted_at,
        _source as _source,
        _batch_id as _batch_id
    from src
)

select * from cleaned
