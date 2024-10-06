{{ config(
    materialized='incremental',
    unique_id='unique_id',
    post_hook="DELETE FROM {{ this }} WHERE status != 'Delivered'"
) }}

SELECT
    unique_id,
    id,
    user_id,
    order_date,
    installments,
    payment_option_id,
    payment_method,
    product_name,
    status,
    seller_name,
    days_until_liquidation,
    currency,
    items_quantity,
    products_value,
    item_fee_value,
    date_liquidation,
    created_at,
    updated_at
FROM {{ ref('sales_info') }} o
{% if is_incremental() %}
WHERE
  o.updated_at >= (
    SELECT COALESCE(MAX(updated_at), DATETIME('1900-01-01')) FROM {{ this }}
  )
{% endif %}

GROUP BY ALL