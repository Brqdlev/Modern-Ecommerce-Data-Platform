{{ config(
    materialized='incremental',
    unique_key='sale_id'
) }}

select
    sale_id,
    store_id,
    product_id,
    sale_date,
    quantity,
    unit_price
from {{ source ('silver', 'store_sales_silver')}}

{% if is_incremental() %}
where _extracted_at > (select max(_extracted_at) from {{ this }})
{% endif %}







