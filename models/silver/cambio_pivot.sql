{{ config(
    materialized='incremental',
    unique_keys='mes'
) }}

-- Para fazer os joins de cambio faz mais sentido ter a moeda como uma coluna categórica

SELECT mes,moeda,taxa_cambio,updated_at from {{ ref('cambio') }}
UNPIVOT(taxa_cambio FOR moeda IN (BRL,
    EUR,
    CNY,
    EGP,
    KRW,
    CLP,
    MXN))
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}


