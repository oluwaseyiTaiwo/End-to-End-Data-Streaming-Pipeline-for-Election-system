from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

DEFAULT_ARGS = {
    "owner": "pipeline",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    dag_id="dbt_refresh",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval="*/5 * * * *",  # runs every 5 minute
    catchup=False,
    tags=["dbt", "bigquery"],
)
def dbt_refresh():
    env_vars = {
        "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",  # Ensure dbt is accessible
        "GOOGLE_APPLICATION_CREDENTIALS": "/keys/gcp_key.json",
        "DBT_PROFILES_DIR": "/opt/airflow/dbt",
    }

    dbt_run_incremental = BashOperator(
        task_id="run_dbt_incremental_models",
        bash_command="""
            cd /opt/airflow/dbt && \
            dbt run --select tag:incremental --profiles-dir .
        """,
        env=env_vars,
    )

    dbt_test_incremental = BashOperator(
        task_id="test_dbt_incremental_models",
        bash_command="""
            cd /opt/airflow/dbt && \
            dbt test --select tag:incremental --profiles-dir .
        """,
        env=env_vars,
    )

    dbt_run_incremental >> dbt_test_incremental

dbt_refresh()
