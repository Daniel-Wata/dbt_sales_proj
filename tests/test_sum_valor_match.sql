-- tests/test_sum_valor_match.sql

WITH
    silver_sum AS (
        SELECT SUM(valor) AS sum_valor
        FROM {{ ref('compras_denormalizado') }}
    ),
    bronze_sum AS (
        SELECT SUM(valor) AS sum_valor
        FROM {{ ref('raw_compras') }}
    )

SELECT
    *
FROM
    silver_sum,
    bronze_sum
WHERE silver_sum.sum_valor != bronze_sum.sum_valor