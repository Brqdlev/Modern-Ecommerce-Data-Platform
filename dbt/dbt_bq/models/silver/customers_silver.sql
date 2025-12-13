{{ config(
    materialized='incremental',
    unique_key='customer_id'
)}}

with src as (
    select
        customer_id,
        customer_name,
        email,
        signup_date,
        region,
        is_vip,
        _extracted_at,
        _source,
        _batch_id
    from {{ source('bronze_bq', 'customers_bronze') }}

    {% if is_incremental() %}
        where _extracted_at > (
            select max(_extracted_at) from {{ this }}
        )
    {% endif %}
),

cleaned as (
    select
        customer_id as customer_id,
        customer_name as customer_name,
        lower(email) as email,
        signup_date as signup_date,
        region as region,
        is_vip as is_vip,
        _extracted_at as _extracted_at,
        _source as _source,
        _batch_id as _batch_id
    from src
)

select * from cleaned
