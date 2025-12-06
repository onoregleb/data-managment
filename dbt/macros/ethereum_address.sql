{% macro ethereum_address(address_column) %}
    -- Макрос для валидации Ethereum адреса
    CASE
        WHEN {{ address_column }} IS NULL THEN NULL
        WHEN LENGTH({{ address_column }}) = 42 AND {{ address_column }} ~ '^0x[a-fA-F0-9]{40}$' THEN {{ address_column }}
        ELSE NULL
    END
{% endmacro %}
