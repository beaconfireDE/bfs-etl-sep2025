from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = "snowflake_conn"
DB = "AIRFLOW0928"
SCHEMA = "DEV"
TABLE = "PRESTAGE_ORDERS_TEAM2"
STAGE = "S3_STAGE_TRANS_ORDER"

DEFAULT_ARGS = {
    "owner": "team2",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="team2_orders_incremental_copy",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 10, 31),
    schedule_interval=None,   
    catchup=False,
    description="Incrementally load new Team2 files from S3 stage into Snowflake"
) as dag:

    incremental_copy = SnowflakeOperator(
        task_id="incremental_copy",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            COPY INTO {DB}.{SCHEMA}.{TABLE}
            FROM @{STAGE}
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_OPTIONALLY_ENCLOSED_BY='"'
                SKIP_HEADER=1
            )
            PATTERN = '.*orders_team2_.*\\.csv'  -- ✅ only Team2
            ON_ERROR = 'CONTINUE';
        """,
    )

    incremental_copy
