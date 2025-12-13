{{ config(
    materialized='incremental',
    unique_key='product_id'
)}}

with src as (
    select
        product_id,
        sku,
        product_name,
        category,
        unit_price,
        unit_cost,
        _extracted_at,
        _source,
        _batch_id
    from {{ source('bronze_bq', 'products_bronze')}}

    {% if is_incremental() %}
        where _extracted_at > (
            select max(_extracted_at) from {{ this }}
        )
    {% endif %}
),

cleaned as (
    select
        CAST(product_id as INT64) as product_id,
        sku as sku,
        product_name as product_name,
        category as category,
        CAST(unit_price as NUMERIC) as unit_price,
        CAST(unit_cost as NUMERIC) as unit_cost,
        CAST(_extracted_at as TIMESTAMP) as _extracted_at,
        _source as _source,
        _batch_id as _batch_id
    from src
)

select * from cleaned

