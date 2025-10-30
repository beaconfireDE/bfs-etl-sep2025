from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

SNOWFLAKE_CONN_ID = "snowflake_conn"

with DAG(
    dag_id="test_snowflake_connection",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["snowflake", "test"],
) as dag:

    check_context = SnowflakeOperator(
        task_id="check_snowflake_context",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
        SELECT 'Connected.';
        """,
    )

    check_context
