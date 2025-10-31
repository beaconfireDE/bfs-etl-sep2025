from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Connection & Table Info
SNOWFLAKE_CONN_ID = "snowflake_conn"
DB = "AIRFLOW0928"
SCHEMA = "DEV"
TABLE = "PRESTAGE_ORDERS_TEAM2"
STAGE = "S3_STAGE_TRANS_ORDER"

# Default args
DEFAULT_ARGS = {
    "owner": "team2",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3)
}

# DAG Definition
with DAG(
    dag_id="team2_orders_incremental_copy",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 10, 24),     # Earliest file date
    end_date=datetime(2025, 10, 26),       # ✅ Temporary: only backfill 3 days
    schedule_interval="0 6 * * *",         # Daily 06:00 UTC
    catchup=True,                          # ✅ Allow backfill for past dates
    description="Daily incremental load from S3 to Snowflake table"
) as dag:

    # Copy matching daily file from S3 stage into Snowflake
    copy_daily_orders = SnowflakeOperator(
        task_id="copy_daily_orders",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            COPY INTO {DB}.{SCHEMA}.{TABLE}
            FROM @{STAGE}
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_OPTIONALLY_ENCLOSED_BY='"'
                SKIP_HEADER=1
            )
            PATTERN = '.*orders_team2_{{{{ ds }}}}.*\\.csv'
            ON_ERROR = 'CONTINUE';
        """,
    )

    copy_daily_orders
