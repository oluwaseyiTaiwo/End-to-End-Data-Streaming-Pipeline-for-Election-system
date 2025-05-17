from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {"owner": "pipeline", "retries": 0}

with DAG(
    dag_id="dbt_refresh",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval="*/1 * * * *",      # every 1 minutes
    catchup=False,
    tags=["dbt", "bigquery"],
) as dag:

    # 1) Run only incremental models (tags keep it fast)
    dbt_run = BashOperator(
        task_id="dbt_run_incremental",
        bash_command="""
            cd /opt/airflow/dbt && \
            dbt run --select tag:incremental --profiles-dir .
        """,
        env={
            "GOOGLE_APPLICATION_CREDENTIALS": "/keys/gcp_key.json",
            "DBT_PROFILES_DIR": "/opt/airflow/dbt",
        },
    )

    # 2) Optionally run tests on the same subset
    dbt_test = BashOperator(
        task_id="dbt_test_incremental",
        bash_command="""
            cd /opt/airflow/dbt && \
            dbt test --select tag:incremental --profiles-dir .
        """,
        env={
            "GOOGLE_APPLICATION_CREDENTIALS": "/keys/gcp_key.json",
            "DBT_PROFILES_DIR": "/opt/airflow/dbt",
        },
    )

    dbt_run >> dbt_test        # test only after a successful run
