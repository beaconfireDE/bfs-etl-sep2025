import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW0928'
SNOWFLAKE_SCHEMA = 'DEV'

SNOWFLAKE_ROLE = 'DE_0928'
SNOWFLAKE_WAREHOUSE = 'DE_0928_WH'
SNOWFLAKE_STAGE = 'Olivia_stage'

with DAG(
    dag_id="test_snowflake_conn",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=True,
) as dag:

    test_conn = SnowflakeOperator(
        task_id="test_snowflake_connection",
        snowflake_conn_id="snowflake_conn",  
        sql="SELECT CURRENT_VERSION();"
    )

    test_conn




