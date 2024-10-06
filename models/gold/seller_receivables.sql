{{ config(
    materialized='table',
) }}

SELECT
    seller_name,
    date_liquidation,
    SUM(products_value) as amount
FROM {{ ref('delivered_sales') }} o
GROUP BY ALL
ORDER BY 1, 2 ASC