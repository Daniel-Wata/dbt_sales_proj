{{ config(
    materialized='table'
) }}

SELECT 
    name,
    categoria,
    SUM(CASE WHEN dias_prazo < 0 THEN ABS(dias_prazo)*0.25
    ELSE 0 END) as multa
FROM {{ ref('compras_denormalizado') }}
GROUP BY name,categoria
ORDER BY multa DESC