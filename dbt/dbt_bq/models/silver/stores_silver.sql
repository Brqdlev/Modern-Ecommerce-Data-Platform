{{ config(
    materialized='incremental',
    unique_key='store_id'
)}}

with src as (
    select
        store_id,
        store_name,
        region,
        open_date,
        city,
        _extracted_at,
        _source,
        _batch_id
    from {{ source('bronze_bq', 'stores_bronze')}}

    {% if is_incremental() %}
        where _extracted_at > (
            select max(_extracted_at) from {{ this }}
        )
    {% endif %}
),

cleaned as (
    select
        CAST(store_id as INT64) as store_id,
        store_name as store_name,
        region as region,
        CAST(open_date as DATE) as open_date,
        city as city,
        CAST(_extracted_at as TIMESTAMP) as _extracted_at,
        _source as _source,
        _batch_id as _batch_id
    from src
)

select * from cleaned

