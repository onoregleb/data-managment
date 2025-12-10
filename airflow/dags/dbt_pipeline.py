"""
DBT Pipeline DAG

Запускает dbt модели и тесты для трансформации данных в Data Warehouse.
"""

import os
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# DBT project directory
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

# PostgreSQL connection from environment
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres-dw")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "blockchain")
POSTGRES_EDR_SCHEMA = os.getenv("POSTGRES_EDR_SCHEMA", "public_edr")

# Set environment variables for dbt
dbt_env = {
    "DBT_PROJECT_DIR": DBT_PROJECT_DIR,
    "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
    "POSTGRES_HOST": POSTGRES_HOST,
    "POSTGRES_USER": POSTGRES_USER,
    "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
    "POSTGRES_PORT": POSTGRES_PORT,
    "POSTGRES_DB": POSTGRES_DB,
    "POSTGRES_EDR_SCHEMA": POSTGRES_EDR_SCHEMA,
}

with DAG(
    "dbt_pipeline",
    default_args=default_args,
    description="DBT: Transform blockchain data in PostgreSQL DWH",
    schedule_interval="*/25 * * * *",  # Запуск каждые 25 минут
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "transform", "blockchain", "postgresql"],
) as dag:

    # Task 1: dbt deps (установка зависимостей)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"export PATH=$PATH:/home/airflow/.local/bin && export PYTHONPATH=$PYTHONPATH:/home/airflow/.local/lib/python3.8/site-packages && cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
        env=dbt_env,
    )

    # Task 1.5: elementary monitor models (dbt run) в встроенном проекте Elementary
    edr_models = BashOperator(
        task_id="edr_models",
        bash_command=(
            "export PATH=$PATH:/home/airflow/.local/bin && "
            "export PYTHONPATH=$PYTHONPATH:/home/airflow/.local/lib/python3.8/site-packages && "
            # Чистим зависшие __dbt_backup перед запуском Elementary
            "PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB "
            '-c "DO \\$\\$ DECLARE r record; BEGIN '
            "FOR r IN (SELECT schemaname, tablename FROM pg_tables WHERE tablename LIKE '%__dbt_backup') LOOP "
            "EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', r.schemaname, r.tablename); "
            'END LOOP; END \\$\\$;" && '
            # Дропаем проблемные relation, если остались от прошлых неудачных запусков
            "PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB "
            '-c "DROP VIEW IF EXISTS public_ods.ods_transactions CASCADE; '
            "DROP VIEW IF EXISTS public_ods.ods_wallets CASCADE; "
            "DO \\$\\$ DECLARE obj RECORD; BEGIN FOR obj IN (SELECT schemaname, tablename as name, 'table' as type FROM pg_tables WHERE schemaname = 'public_edr' AND tablename IN ('anomaly_threshold_sensitivity', 'alerts_dbt_models') UNION ALL SELECT schemaname, viewname as name, 'view' as type FROM pg_views WHERE schemaname = 'public_edr' AND viewname IN ('anomaly_threshold_sensitivity', 'alerts_dbt_models')) LOOP IF obj.type = 'table' THEN EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', obj.schemaname, obj.name); ELSE EXECUTE format('DROP VIEW IF EXISTS %I.%I CASCADE', obj.schemaname, obj.name); END IF; END LOOP; END \\$\\$;\" && "
            "cd /home/airflow/.local/lib/python3.8/site-packages/elementary/monitor/dbt_project && "
            "dbt run --profiles-dir /opt/airflow/dbt --target prod --full-refresh"
        ),
        env=dbt_env,
    )

    # Task 2: dbt run (запуск моделей)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"export PATH=$PATH:/home/airflow/.local/bin && export PYTHONPATH=$PYTHONPATH:/home/airflow/.local/lib/python3.8/site-packages && cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR} --target prod",
        env=dbt_env,
    )

    # Task 3: dbt test (запуск тестов)
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"export PATH=$PATH:/home/airflow/.local/bin && export PYTHONPATH=$PYTHONPATH:/home/airflow/.local/lib/python3.8/site-packages && cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR} --target prod",
        env=dbt_env,
    )

    edr_report = BashOperator(
        task_id="edr_report",
        bash_command=(
            f"export PATH=$PATH:/home/airflow/.local/bin && "
            f"export PYTHONPATH=$PYTHONPATH:/home/airflow/.local/lib/python3.8/site-packages && "
            f"cd {DBT_PROJECT_DIR} && "
            f"mkdir -p edr_reports && "
            f"edr report --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --profile-target prod --target-path edr_reports"
        ),
        env=dbt_env,
    )

    # Pipeline: deps -> run -> test -> edr report
    dbt_deps >> edr_models >> dbt_run >> dbt_test >> edr_report
