import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
AWS_CONN_ID = "aws_default"
S3_BUCKET = "de-batch-sep2025"
S3_PREFIX = "Olivia"

SNOWFLAKE_DATABASE = 'AIRFLOW0928'
SNOWFLAKE_SCHEMA = 'DEV'
SNOWFLAKE_ROLE = 'DE_0928'
SNOWFLAKE_WAREHOUSE = 'DE_0928_WH'
SNOWFLAKE_STAGE = 'S3_STAGE_TRANS_ORDER'
group = 'Group01'
DATES = ["2025-06-30", "2025-07-01", "2025-07-02"]



with DAG(
    dag_id="test_airflow_s3_snowflake_connections",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # 🧩 1. Test AWS S3 Connection
    @task()
    def test_s3_conn():
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        files = hook.list_keys(bucket_name=S3_BUCKET, prefix=S3_PREFIX)
        if files:
            print("✅ S3 Connection successful. Found files:", files[:3])
        else:
            raise ValueError("⚠️ S3 connected but no files found.")
    
    # 🧩 2. Test Snowflake Connection
    test_snowflake_conn = SnowflakeOperator(
        task_id="test_snowflake_conn",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="SELECT CURRENT_VERSION();"
    )

    test_s3_conn() >> test_snowflake_conn

