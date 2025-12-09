{% macro drop_dbt_backups() %}
    {#-
      Удаляет «залипшие» объекты вида *_dbt_backup, которые остаются,
      если предыдущий dbt run упал до завершения атомарной замены.
    -#}
    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {# выбираем все таблицы/вью с суффиксом __dbt_backup #}
    {% set sql %}
        select table_schema, table_name
        from information_schema.tables
        where table_name ilike '%\_\_dbt_backup'
    {% endset %}

    {% set backups = run_query(sql) %}

    {% if backups and backups.rows %}
        {% for row in backups %}
            {% set rel = adapter.get_relation(
                database=target.database,
                schema=row['table_schema'],
                identifier=row['table_name']
            ) %}
            {% if rel %}
                {{ log('Dropping stale backup ' ~ rel, info=True) }}
                {% do adapter.drop_relation(rel) %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}
