{% macro date_parse(date_column) %}
CASE
    -- Convert dates in YYYY-MM-DD format
    WHEN SAFE_CAST({{ date_column }} AS DATE) IS NOT NULL THEN SAFE_CAST({{ date_column }} AS DATE)
    -- Convert dates in DD/MM/YYYY format
    WHEN SAFE.PARSE_DATE('%d/%m/%Y', {{ date_column }}) IS NOT NULL THEN PARSE_DATE('%d/%m/%Y', {{ date_column }})
    ELSE SAFE.PARSE_DATE('%d-%m-%Y', {{ date_column }})
END
{% endmacro %}