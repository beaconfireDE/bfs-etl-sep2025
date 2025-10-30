from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
from pathlib import Path
import os
import re

# -----------------------------------------------------------------------------
# DAG CONFIGURATION
# -----------------------------------------------------------------------------
default_args = {
    "owner": "team3",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 30),
    "retries": 1,
}

SNOWFLAKE_CONN_ID = "snowflake_conn"
SNOWFLAKE_ROLE = "DE_DEVELOPER_0928"
SNOWFLAKE_WAREHOUSE = "DE_0928_WH"
SNOWFLAKE_DATABASE = "AIRFLOW0928"
SNOWFLAKE_SCHEMA = "DEV"


SRC_PATH = Path(__file__).parent / "airflow0928_dev_sql"
TEAM_SUFFIX = "_team3"

# -----------------------------------------------------------------------------
# Helper: Load SQL file and replace table names
# -----------------------------------------------------------------------------
def load_sql(file_name: str) -> str:
    """Reads SQL and appends _team3 suffix to target tables."""
    file_path = os.path.join(SRC_PATH, file_name)
    with open(file_path, "r", encoding="utf-8") as f:
        sql = f.read()

    for tbl in ["dim_company", "dim_date", "dim_symbol", "fact_market_daily"]:
        pattern = rf"\b{tbl}\b"
        sql = re.sub(pattern, f"{tbl}{TEAM_SUFFIX}", sql)

    return sql

# -----------------------------------------------------------------------------
# DAG DEFINITION
# -----------------------------------------------------------------------------
with DAG(
    dag_id="etl_team3_snowflake_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="ETL automation DAG for Snowflake tables (Team3)",
    tags=["snowflake", "etl", "team3"],
) as dag:

    # ------------------------- CREATE TASKS ----------------------------------
    create_dim_company = SnowflakeOperator(
        task_id="create_dim_company",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        sql=load_sql("create_dim_company.sql"),
    )

    create_dim_date = SnowflakeOperator(
        task_id="create_dim_date",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        sql=load_sql("create_dim_date.sql"),
    )

    create_dim_symbol = SnowflakeOperator(
        task_id="create_dim_symbol",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        sql=load_sql("create_dim_symbol.sql"),
    )

    create_fact_market_daily = SnowflakeOperator(
        task_id="create_fact_market_daily",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        sql=load_sql("create_fact_market_daily.sql"),
    )

    # ------------------------- UPDATE TASKS ----------------------------------
    update_dim_company = SnowflakeOperator(
        task_id="update_dim_company",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        sql=load_sql("update_dim_company.sql"),
    )

    update_dim_date = SnowflakeOperator(
        task_id="update_dim_date",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        sql=load_sql("update_dim_date.sql"),
    )

    update_dim_symbol = SnowflakeOperator(
        task_id="update_dim_symbol",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        sql=load_sql("update_dim_symbol.sql"),
    )

    update_fact_market_daily = SnowflakeOperator(
        task_id="update_fact_market_daily",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        sql=load_sql("update_fact_market_daily .sql"),
    )

    # ------------------------- TASK DEPENDENCIES -----------------------------
    (
        [create_dim_company, create_dim_date, create_dim_symbol]
        >> create_fact_market_daily
        >> [update_dim_company, update_dim_date, update_dim_symbol]
        >> update_fact_market_daily
    )
