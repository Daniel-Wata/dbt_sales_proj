{{ config(
    materialized='incremental',
    unique_key='id'
) }}

SELECT
    id,
    name,
    email,
    address,
    REGEXP_EXTRACT(address, r', ([A-Za-z ]+),') AS city,
    REGEXP_EXTRACT(address, r', ([A-Z]{2}) ') AS state,
    REGEXP_EXTRACT(address, r' (\d{5})$') AS zipcode,
    birthdate,
    created_at,
    updated_at
    FROM {{ source('dbt_sales_proj_raw','users')}}
{% if is_incremental() %}
  where updated_at >= (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
{% endif %}