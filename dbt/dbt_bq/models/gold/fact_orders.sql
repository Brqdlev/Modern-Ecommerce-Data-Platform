{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}


select
    order_id,
    customer_id,
    order_date,
    order_status,
    shipping_cost,
    payment_method,
from {{ source('silver','order_silver')}}

{% if is_incremental() %}
where _extracted_at > (select max(_extracted_at) from {{ this }})
{% endif %}
