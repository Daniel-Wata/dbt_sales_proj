{{ config(
    materialized='table'
) }}

SELECT 
    name as nome_cliente,
    array_agg(DISTINCT categoria) as categorias_compradas_arr,
    string_agg(DISTINCT categoria, ",") as categorias_compradas_str
FROM {{ ref('compras_denormalizado') }}
GROUP BY name
