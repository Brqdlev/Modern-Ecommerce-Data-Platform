{{ config(
    materialized='incremental',
    unique_key='order_item_id'
) }}

select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount_amount,
    total_price
from {{ source('silver', 'order_items_silver')}}

{% if is_incremental() %}
where _extracted_at > (select max(_extracted_at) from {{ this }})
{% endif %}
