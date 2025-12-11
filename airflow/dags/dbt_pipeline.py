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
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
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

# Serialize dbt runs across DAGs (LocalExecutor may run tasks concurrently).
# All dbt-related tasks will acquire this file lock inside the Airflow container.
DBT_LOCK_FILE = os.getenv("DBT_LOCK_FILE", "/tmp/dbt_global.lock")

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
        bash_command=(
            f"export PATH=$PATH:/home/airflow/.local/bin && "
            f"export PYTHONPATH=$PYTHONPATH:/home/airflow/.local/lib/python3.11/site-packages && "
            f"flock -w 1800 {DBT_LOCK_FILE} bash -c "
            f'"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}"'
        ),
        env=dbt_env,
    )

    # Task 1.5: elementary monitor models (dbt run) в встроенном проекте Elementary
    edr_models = BashOperator(
        task_id="edr_models",
        bash_command=(
            "export PATH=$PATH:/home/airflow/.local/bin && "
            "export PYTHONPATH=$PYTHONPATH:/home/airflow/.local/lib/python3.11/site-packages && "
            # Serialize cleanup + Elementary run to avoid concurrent dbt runs across DAGs.
            f'flock -w 1800 {DBT_LOCK_FILE} bash -c "'
            # Чистим зависшие __dbt_backup (и таблицы, и view/materialized view), плюс проблемные relation
            "PGPASSWORD=\\$POSTGRES_PASSWORD psql -h \\$POSTGRES_HOST -p \\$POSTGRES_PORT -U \\$POSTGRES_USER -d \\$POSTGRES_DB "
            '-c \\"DO \\\\\\$\\\\\\$ DECLARE r record; BEGIN '
            "FOR r IN ("
            "  SELECT n.nspname AS schemaname, c.relname AS name, c.relkind "
            "  FROM pg_catalog.pg_class c "
            "  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            "  WHERE c.relname LIKE '%__dbt_backup' "
            "     OR (n.nspname IN ('public_ods','ods') AND c.relname IN ('ods_transactions','ods_wallets')) "
            "     OR (n.nspname IN ('\\$POSTGRES_EDR_SCHEMA','public_edr') AND c.relname IN ("
            "       'elementary_test_results','schema_columns_snapshot','metadata','metrics_anomaly_score',"
            "       'monitors_runs','job_run_results','anomaly_threshold_sensitivity','alerts_dbt_models'"
            "     ))"
            ") LOOP "
            "  IF r.relkind = 'v' THEN "
            "    EXECUTE format('DROP VIEW IF EXISTS %I.%I CASCADE', r.schemaname, r.name); "
            "  ELSIF r.relkind = 'm' THEN "
            "    EXECUTE format('DROP MATERIALIZED VIEW IF EXISTS %I.%I CASCADE', r.schemaname, r.name); "
            "  ELSE "
            "    EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', r.schemaname, r.name); "
            "  END IF; "
            "END LOOP; "
            'END \\\\\\$\\\\\\$;\\" && '
            "cd /home/airflow/.local/lib/python3.11/site-packages/elementary/monitor/dbt_project && "
            "dbt run --profiles-dir /opt/airflow/dbt --target prod --full-refresh"
            '"'
        ),
        env=dbt_env,
    )

    # Task 2: dbt run (запуск моделей)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"export PATH=$PATH:/home/airflow/.local/bin && "
            f"export PYTHONPATH=$PYTHONPATH:/home/airflow/.local/lib/python3.11/site-packages && "
            f"flock -w 1800 {DBT_LOCK_FILE} bash -c "
            f'"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR} --target prod"'
        ),
        env=dbt_env,
    )

    # Task 3: dbt test (запуск тестов)
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"export PATH=$PATH:/home/airflow/.local/bin && "
            f"export PYTHONPATH=$PYTHONPATH:/home/airflow/.local/lib/python3.11/site-packages && "
            f"flock -w 1800 {DBT_LOCK_FILE} bash -c "
            f'"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR} --target prod"'
        ),
        env=dbt_env,
    )

    edr_report = BashOperator(
        task_id="edr_report",
        bash_command=(
            f"export PATH=$PATH:/home/airflow/.local/bin && "
            f"export PYTHONPATH=$PYTHONPATH:/home/airflow/.local/lib/python3.11/site-packages && "
            f"flock -w 1800 {DBT_LOCK_FILE} bash -c "
            f'"cd {DBT_PROJECT_DIR} && mkdir -p edr_reports && '
            f"edr report --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} "
            f'--profile-target prod --target-path edr_reports"'
        ),
        env=dbt_env,
    )

    # Pipeline: deps -> run -> test -> edr report
    dbt_deps >> edr_models >> dbt_run >> dbt_test >> edr_report
