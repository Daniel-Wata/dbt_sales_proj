{{ config(
    materialized='incremental',
    unique_key=['id']
) }}

SELECT
    id,
    order_id,
    product_id,
    seller_id,
    price,
    fee_value,
    created_at,
    updated_at
FROM {{ source('dbt_sales_proj_raw', 'order_items') }} 

{% if is_incremental() %}
  where updated_at >= (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
{% endif %}