{% macro get_max_datetime(table_aliases, datetime_columns) %}
GREATEST(
  {% for i in range(table_aliases | length) %}
    {{ table_aliases[i] }}.{{ datetime_columns[i] }}{% if not loop.last %}, {% endif %}
  {% endfor %}
)
{% endmacro %}