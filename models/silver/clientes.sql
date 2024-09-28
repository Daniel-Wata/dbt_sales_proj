{{ config(
    materialized='incremental',
    unique_key='id'
) }}

-- Aqui pensei em limpar o mínimo na chegada pra camada silver. Tratei as datas e limpei duplicatas
--Criar testes para as datas, não vejo muito pq criar teste para a duplicata porque estamos pegando sempre o máximo (sempre vai retornar uma linha)
with datas_normalizadas AS (
SELECT 
id,
name,
pais,
{{ date_parse('data_nascimento') }} data_nascimento,
updated_at
from {{ ref('raw_clientes') }}
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
)
,

dados_ordenados AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS row_num
    FROM datas_normalizadas
)

SELECT
    id,
    name,
    pais,
    data_nascimento,
    updated_at
FROM dados_ordenados
WHERE row_num = 1