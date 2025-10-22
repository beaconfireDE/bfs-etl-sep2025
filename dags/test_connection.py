from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

with DAG(
    dag_id="test_snowflake_conn",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    test_conn = SnowflakeOperator(
        task_id="test_snowflake_connection",
        snowflake_conn_id="snowflake_conn",  
        sql="SELECT CURRENT_VERSION();"
    )

    test_conn
