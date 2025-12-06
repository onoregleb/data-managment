{% macro generate_surrogate_key(field_list) %}
    -- Генерация суррогатного ключа из списка полей
    {{ dbt_utils.generate_surrogate_key(field_list) }}
{% endmacro %}
