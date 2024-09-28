{{ config(
    materialized='table'
) }}

SELECT 
    categoria,
    SUM(valor_dolar) as valor_pendente_dolar
FROM {{ ref('compras_denormalizado') }}
WHERE status = 'Pendente'
GROUP BY categoria
ORDER BY valor_pendente_dolar DESC