{{ config(
    materialized='table'
) }}

SELECT 
    categoria,
    COUNT(id ) as num_compras
FROM {{ ref('compras_denormalizado') }}
WHERE dias_prazo < 0
GROUP BY categoria
ORDER BY COUNT(id )  desc