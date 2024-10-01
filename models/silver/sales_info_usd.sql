{{ config(
    materialized='incremental',
    unique_id='unique_id'
) }}

SELECT
    unique_id,
    id,
    user_id,
    order_date,
    installments,
    status_id,
    status,
    payment_option_id,
    payment_method,
    product_name,
    seller_name,
    days_until_liquidation,
    o.currency,
    items_quantity,
    products_value,
    item_fee_value,
    IF(o.currency='USD',products_value,products_value*c.exchange_rate_to_usd) as products_value_usd,
    IF(o.currency='USD',item_fee_value,item_fee_value*c.exchange_rate_to_usd) as item_fee_value_usd,
    created_at,
    updated_at
FROM {{ ref('sales_info') }} o
LEFT JOIN {{ source('dbt_sales_proj_raw','currency_exchange_rates') }} c ON c.date = o.order_date AND c.currency = o.currency
{% if is_incremental() %}
WHERE
  o.updated_at >= (
    SELECT COALESCE(MAX(updated_at), DATETIME('1900-01-01')) FROM {{ this }}
  )
{% endif %}

GROUP BY ALL