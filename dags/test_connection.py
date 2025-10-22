import os
from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# --- Configuration ---
SNOWFLAKE_CONN_ID = "snowflake_conn"
AWS_CONN_ID = "aws_default"

S3_BUCKET = "de-batch-sep2025"
S3_PREFIX = "Olivia"

SNOWFLAKE_DATABASE = "AIRFLOW0928"
SNOWFLAKE_SCHEMA = "DEV"
SNOWFLAKE_ROLE = "DE_0928"
SNOWFLAKE_WAREHOUSE = "DE_0928_WH"
SNOWFLAKE_STAGE = "S3_STAGE_TRANS_ORDER"

GROUP = "Group01"
TABLE_NAME = f"prestage_orders_{GROUP}"
DATES = ["2025-06-30", "2025-07-01", "2025-07-02"]

default_args = {"owner": "airflow"}

# --- DAG Definition ---
with DAG(
    dag_id="load_to_snowflake_Group01",
    start_date=datetime(2025, 6, 30),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["snowflake", "s3"],
) as dag:

    # 🧩 Step 1: Create target table if not exists
    create_table = SnowflakeOperator(
        task_id="create_table",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TABLE_NAME} (
            order_id INT,
            customer_id STRING,
            product_id STRING,
            quantity INT,
            unit_price FLOAT,
            order_date DATE,
            ship_date DATE,
            warehouse_code STRING,
            sales_channel STRING,
            currency_code STRING
        );
        """,
    )

    # 🧩 Step 2: For each day, wait for S3 file → then copy into Snowflake
    for d in DATES:
        file_name = f"Filename_{GROUP}_{d}.csv"
        s3_key = f"{S3_PREFIX}/{file_name}"

        wait_for_file = S3KeySensor(
            task_id=f"wait_for_{d}_file",
            bucket_key=s3_key,
            bucket_name=S3_BUCKET,
            aws_conn_id=AWS_CONN_ID,
            poke_interval=60,
            timeout=600,
        )

        load_file = SnowflakeOperator(
            task_id=f"copy_{d}_into_snowflake",
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql=f"""
            COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TABLE_NAME}
            FROM @{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}
            PATTERN = '.*{file_name}.*'
            ON_ERROR = 'CONTINUE';
            """,
        )

        create_table >> wait_for_file >> load_file


# with DAG(
#     dag_id="test_airflow_s3_snowflake_connections",
#     start_date=datetime(2025, 1, 1),
#     schedule_interval=None,
#     catchup=False,
# ) as dag:

#     # 🧩 1. Test AWS S3 Connection
#     @task()
#     def test_s3_conn():
#         hook = S3Hook(aws_conn_id=AWS_CONN_ID)
#         files = hook.list_keys(bucket_name=S3_BUCKET, prefix=S3_PREFIX)
#         if files:
#             print("✅ S3 Connection successful. Found files:", files[:3])
#         else:
#             raise ValueError("⚠️ S3 connected but no files found.")
    
#     # 🧩 2. Test Snowflake Connection
#     test_snowflake_conn = SnowflakeOperator(
#         task_id="test_snowflake_conn",
#         snowflake_conn_id=SNOWFLAKE_CONN_ID,
#         sql="SELECT CURRENT_VERSION();"
#     )

#     test_s3_conn() >> test_snowflake_conn

