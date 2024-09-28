{{ config(
    materialized='incremental',
    unique_key='mes'
) }}

SELECT
    mes,
    BRL,
    EUR,
    CNY,
    EGP,
    KRW,
    CLP,
    MXN,
    updated_at
FROM {{ source('ebanx_raw', 'e_cambio') }}
WHERE mes is not null
{% if is_incremental() %}
and updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}