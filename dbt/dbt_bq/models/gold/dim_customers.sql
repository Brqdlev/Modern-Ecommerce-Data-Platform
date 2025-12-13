{{ config(
    materialized='table'
)}}

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
from {{ source('silver', 'customers_silver') }}