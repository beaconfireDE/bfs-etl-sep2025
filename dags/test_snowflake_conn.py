from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

with DAG(
    dag_id="test_snowflake_conn",
    start_date=datetime(2025, 10, 28),   
    schedule_interval=None,             
    catchup=False,
) as dag:

    test_query = SnowflakeOperator(
        task_id="test_connection",
        snowflake_conn_id="snowflake_conn",
        sql="SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA();"
        
    )
