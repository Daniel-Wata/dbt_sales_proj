{{ config(
    materialized='table'
) }}

SELECT
    o.id,
    o.user_id,
    o.order_date,
    COALESCE(o.installments,1) as installments,
    o.status_id,
    s.status_name as status,
    o.payment_option_id,
    po.payment_method,
    products_value,
    fee_value,
    interest_fee,
    total_transaction_value
FROM {{ source('dbt_sales_proj_raw', 'orders') }} o
LEFT JOIN {{ source('dbt_sales_proj_raw', 'status') }} s ON o.status_id = s.id 
LEFT JOIN {{ source('dbt_sales_proj_raw','payment_option')}} po ON po.id = o.payment_option_id