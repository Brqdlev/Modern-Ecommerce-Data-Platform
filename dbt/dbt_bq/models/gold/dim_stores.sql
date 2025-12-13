{{ config(materialized = 'table')}}

select
    store_id,
    store_name,
    region,
    open_date,
    city
from {{ source('silver', 'stores_silver')}}