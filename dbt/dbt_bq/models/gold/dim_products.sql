{{ config(materialized = 'table')}}

select
    product_id,
    sku,
    product_name,
    category,
    unit_price,
    unit_cost
from {{ source ('silver', 'products_silver')}}