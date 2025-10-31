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
    start_date=datetime(2025, 10, 24),  # 从 10/24 开始
    end_date=datetime(2025, 10, 26),    # 临时补跑到 10/26
    schedule_interval="0 6 * * *",      # 每天早上 6 点
    catchup=True,                       # ✅ 允许补历史
    description="Daily incremental load from S3 to Snowflake table"
) as dag:

    # 使用 SnowflakeOperator 执行 COPY
    copy_daily_orders = SnowflakeOperator(
        task_id="copy_daily_orders",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            -- 打印当前执行日期，帮助调试
            SELECT '{{{{ ds }}}}' AS execution_date;

            -- 实际 COPY 操作
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
