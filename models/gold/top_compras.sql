{{ config(
    materialized='table'
) }}

SELECT 
    name,
    pais,
    MAX(valor_dolar ) as maior_compra
FROM {{ ref('compras_denormalizado') }}
GROUP BY name,pais
ORDER BY MAX(valor_dolar ) desc