# DBT Project: Blockchain Data Warehouse

Проект dbt для трансформации данных блокчейна в Data Warehouse.

## Структура проекта

```
dbt/
├── dbt_project.yml      # Конфигурация проекта
├── profiles.yml          # Конфигурация подключений
├── packages.yml         # Зависимости (dbt_utils)
├── macros/              # Пользовательские макросы
│   ├── ethereum_address.sql
│   ├── eth_to_wei.sql
│   └── generate_surrogate_key.sql
├── models/
│   ├── sources.yml      # Определение источников данных
│   ├── schema.yml       # Схема и тесты для моделей
│   ├── staging/         # Staging модели (views)
│   │   ├── stg_wallets.sql
│   │   └── stg_transactions.sql
│   ├── ods/             # ODS слой (views с валидацией и окнами)
│   │   ├── ods_wallets.sql
│   │   └── ods_transactions.sql
│   ├── intermediate/    # Промежуточные модели (views)
│   │   ├── int_wallet_transactions.sql
│   │   └── int_daily_transactions.sql
│   └── marts/           # Мартовые модели (tables)
│       ├── fct_wallet_activity.sql
│       ├── dim_wallets.sql
│       └── daily_transaction_summary.sql
└── README.md
```

## Модели

### Staging (staging/)
- **stg_wallets**: Очистка и переименование данных кошельков
- **stg_transactions**: Очистка и переименование данных транзакций

### ODS (ods/)
- **ods_wallets**: Валидированные кошельки с адресами и метаданными
- **ods_transactions**: Транзакции с оконными метриками (cumulative volume, lag)

### Intermediate (intermediate/)
- **int_wallet_transactions**: Агрегация статистики по кошелькам
- **int_daily_transactions**: Дневная агрегация транзакций

### Marts (marts/)
- **fct_wallet_activity**: Фактовая таблица активности кошельков
- **dim_wallets**: Измерение кошельков с метриками
- **daily_transaction_summary**: Дневная сводка транзакций

## Макросы

- `ethereum_address()`: Валидация Ethereum адресов
- `eth_to_wei()`: Конвертация ETH в Wei
- `generate_surrogate_key()`: Генерация суррогатных ключей

## Запуск

### Локально

```bash
cd dbt
dbt deps          # Установка зависимостей
dbt run           # Запуск моделей
dbt test          # Запуск тестов
```

### В Airflow

DBT задачи автоматически запускаются в DAG `el_mongo_to_postgres` после загрузки данных.

## Тесты

Все модели имеют встроенные тесты:
- `unique`: Уникальность ключей
- `not_null`: Проверка на NULL значения

Тесты определены в `models/sources.yml` и `models/schema.yml`.
