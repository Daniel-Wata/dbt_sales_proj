{{ config(
    materialized='incremental',
    unique_key='id'
) }}

SELECT
    id,
    name,
    pais,
    data_nascimento,
    updated_at
FROM {{ source('ebanx_raw', 'e_clientes') }}
WHERE id is not null
{% if is_incremental() %}
and updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}