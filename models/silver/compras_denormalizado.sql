{{ config(
    materialized='incremental',
    unique_key='id',
    partition_by={
        "field": "data_pagamento",
        "data_type": "DATE"
    },
    cluster_by=['status', 'categoria']
) }}

-- Pelos dados que encontrei, estamos só com problema nos nulls em data_pagamento

SELECT
    compras.id,
    id_cliente,
    clientes.name,
    clientes.pais,
    data_vencimento,
    data_pagamento,
    valor,
    categoria,
    compras.moeda,
    status,
    GREATEST(COALESCE(compras.updated_at, DATE '2000-01-01'),COALESCE(cambio.updated_at, DATE '2000-01-01'),COALESCE(clientes.updated_at, DATE '2000-01-01')) as updated_at,
    DATE_DIFF(data_vencimento,COALESCE(data_pagamento,CURRENT_DATE()),DAY) as dias_prazo,
    COALESCE(taxa_cambio,1) as taxa_cambio,
    CASE WHEN
        compras.moeda = 'USD' THEN valor
        WHEN taxa_cambio is not null THEN valor*taxa_cambio
        ELSE NULL END AS valor_dolar
from {{ ref('compras') }} compras
LEFT JOIN {{ ref('cambio_pivot') }} cambio ON cambio.mes = EXTRACT(month from COALESCE(data_pagamento,data_vencimento)) and cambio.moeda = compras.moeda
LEFT JOIN {{ ref('clientes') }} clientes on clientes.id = compras.id_cliente
{% if is_incremental() %}
WHERE clientes.updated_at > (SELECT MAX(updated_at) FROM {{ this }}) OR compras.updated_at > (SELECT MAX(updated_at) FROM {{ this }}) OR cambio.updated_at > (SELECT MAX(updated_at) FROM {{ this }}) OR data_pagamento is null --sempre atualiza o que não estiver pago por conta da virada de dia
{% endif %}