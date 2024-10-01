{{ config(
    materialized='incremental',
    unique_key='id'
) }}

SELECT
    id,
    order_id,
    order_item_id,
    rating,
    comment,
    created_at,
    updated_at,
    deleted_at
FROM {{ source('dbt_sales_proj_raw', 'ratings') }} 

{% if is_incremental() %}
  where updated_at >= (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
{% endif %}