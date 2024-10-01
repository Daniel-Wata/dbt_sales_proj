{{ config(
    materialized='incremental',
    unique_id='unique_id'
) }}

SELECT
    CONCAT(cast(o.id as string),"_",cast(oi.id as string)) as unique_id,
    o.id,
    o.user_id,
    o.order_date,
    o.installments,
    o.status_id,
    o.status,
    o.payment_option_id,
    o.payment_method,
    o.created_at,
    o.updated_at,
    p.product_name,
    sc.seller_name,
    sc.days_until_liquidation,
    sc.currency,
    COUNT(oi.id) as items_quantity,
    SUM(oi.price) as products_value, 
    SUM(oi.fee_value) as item_fee_value
FROM {{ ref('orders') }} o
INNER JOIN {{ref('order_items')}} oi on oi.order_id = o.id
INNER JOIN {{ref('products')}} p on p.id = oi.product_id
INNER JOIN {{ref('seller_conditions')}} sc on p.seller_id = sc.id

{% if is_incremental() %}
WHERE
  {{ get_max_datetime(['o', 'oi', 'p', 'sc'], ['updated_at', 'updated_at', 'updated_at', 'updated_at']) }} >= (
    SELECT COALESCE(MAX(updated_at), DATETIME('1900-01-01')) FROM {{ this }}
  )
{% endif %}

GROUP BY ALL