{{ config(
    materialized='incremental',
    unique_key='id'
) }}

SELECT
    id,
    product_name,
    category,
    seller_id,
    price,
    created_at,
    updated_at
FROM {{ source('dbt_sales_proj_raw', 'products') }} 

{% if is_incremental() %}
  where updated_at >= (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
{% endif %}