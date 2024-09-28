{{ config(
    materialized='incremental',
    unique_key='id'
) }}

SELECT
    id,
    id_cliente,
    data_vencimento,
    data_pagamento,
    valor,
    categoria,
    moeda,
    status,
    updated_at
FROM {{ source('ebanx_raw', 'e_compras') }}
WHERE id is not null -- Fonte externa de google sheets sempre pega as linhas vazias
{% if is_incremental() %}
and updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}