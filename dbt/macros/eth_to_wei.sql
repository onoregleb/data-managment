{% macro eth_to_wei(eth_value) %}
    -- Конвертация ETH в Wei (1 ETH = 10^18 Wei)
    {{ eth_value }} * 1000000000000000000
{% endmacro %}
