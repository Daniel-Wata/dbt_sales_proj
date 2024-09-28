{{ config(
    materialized='incremental',
    unique_key='id'
) }}

-- Pelos dados que encontrei, estamos só com problema nos nulls em data_pagamento


SELECT
    id,
    id_cliente,
    data_vencimento,
    {{ date_parse('data_pagamento') }} data_pagamento,
    valor,
    categoria,
    moeda,
    status,
    updated_at
from {{ ref('raw_compras') }}
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
