{{ config(
    materialized='incremental',
    unique_key='mes'
) }}

-- Aqui vou colocar o mes em numérico para facilitar futuros joins (estranhei não ter o ano, mas acredito que seja para simplificar os dados então vou ignorar)

SELECT
    CASE
            WHEN mes = 'Janeiro' THEN 1
            WHEN mes = 'Fevereiro' THEN 2
            WHEN mes = 'Março' THEN 3
            WHEN mes = 'Abril' THEN 4
            WHEN mes = 'Maio' THEN 5
            WHEN mes = 'Junho' THEN 6
            WHEN mes = 'Julho' THEN 7
            WHEN mes = 'Agosto' THEN 8
            WHEN mes = 'Setembro' THEN 9
            WHEN mes = 'Outubro' THEN 10
            WHEN mes = 'Novembro' THEN 11
            WHEN mes = 'Dezembro' THEN 12
            ELSE NULL
        END AS mes,
    BRL,
    EUR,
    CNY,
    EGP,
    KRW,
    CLP,
    MXN,
    updated_at
from {{ ref('raw_cambio') }}
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}

