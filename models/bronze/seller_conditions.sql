{{ config(
    materialized='incremental',
    unique_key='id'
) }}

SELECT
    id,
    seller_name,
    fee_percentage,
    currency,
    days_until_liquidation,
    created_at,
    updated_at
FROM {{ source('dbt_sales_proj_raw', 'seller_conditions') }} 

{% if is_incremental() %}
  where updated_at >= (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
{% endif %}