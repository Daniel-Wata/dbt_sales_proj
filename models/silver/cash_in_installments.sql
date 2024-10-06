{{ config(
    materialized='incremental',
    unique_id='unique_id',
    post_hook="DELETE FROM {{ this }} WHERE status NOT IN ('Approved','Delivered')"
) }}

WITH
  sale_array AS (
  SELECT
    unique_id,
    order_date,
    status,
    installments AS total_installments,
    (GENERATE_ARRAY(1,installments)) AS installments_array,
    products_value + item_fee_value AS total_value,
    ROUND((products_value + item_fee_value)/installments,2) AS regular_installment_value,
    ROUND(products_value + item_fee_value - ROUND((products_value + item_fee_value)/installments,2)*(installments - 1),2) AS final_installment_value,
    updated_at
  FROM {{ ref('sales_info') }}
{% if is_incremental() %}
WHERE
  updated_at >= (
    SELECT COALESCE(MAX(updated_at), DATETIME('1900-01-01')) FROM {{ this }}
  )
{% endif %} )
SELECT
  unique_id,
  order_date,
  installment_number,
  CASE
    WHEN sale_array.total_installments = installment_number THEN sale_array.final_installment_value
    ELSE sale_array.regular_installment_value
END
  AS installment_value,
  status,
  updated_at
FROM
  sale_array,
  UNNEST(installments_array) AS installment_number ORDER BY 1 ASC