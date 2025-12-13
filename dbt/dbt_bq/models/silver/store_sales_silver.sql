{{ config(
    materialized='incremental',
    unique_key='sale_id'
)}}

with src as (
    select
        sale_id,
        store_id,
        product_id,
        sale_date,
        quantity,
        unit_price,
        _extracted_at,
        _source,
        _batch_id
    from {{ source('bronze_bq', 'store_sales_bronze')}}

    {% if is_incremental() %}
        where _extracted_at > (
            select max(_extracted_at) from {{ this }}
        )
    {% endif %}
),

cleaned as (
    select
        CAST(sale_id as INT64) as sale_id,
        CAST(store_id as INT64) as store_id,
        CAST(product_id as INT64) as product_id,
        CAST(sale_date as TIMESTAMP) as sale_date,
        CAST(quantity as INT64) as quantity,
        CAST(unit_price as NUMERIC) as unit_price,
        CAST(_extracted_at as TIMESTAMP) as _extracted_at,
        _source as _source,
        _batch_id as _batch_id
    from src
)

select * from cleaned

