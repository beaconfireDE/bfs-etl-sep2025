from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
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
# -------------------------------------------------------------------------
# Generic Validation Checker
# -------------------------------------------------------------------------
def run_validation(file_name: str):
    """Execute validation SQL and fail DAG if any FAIL or issues > 0."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    sql = load_sql(file_name)
    cursor.execute(sql)
    results = cursor.fetchall()
    col_names = [desc[0] for desc in cursor.description]

    fail_rows = []
    for row in results:
        row_dict = dict(zip(col_names, row))
        for val in row_dict.values():
            if (isinstance(val, str) and val.strip().upper() == "FAIL") or (
                isinstance(val, (int, float)) and val > 0
            ):
                fail_rows.append(row_dict)
                break

    cursor.close()
    conn.close()

    if fail_rows:
        raise AirflowException(f"Validation failed in {file_name}:\n{fail_rows}")
    else:
        print(f"{file_name} passed all data validation checks.")

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
        sql=load_sql("update_fact_market_daily.sql"),
    )

    # ------------------------- VALIDATION TASKS ------------------------------
    validate_consistency = PythonOperator(
        task_id="validate_consistency_sampling",
        python_callable=lambda **_: run_validation("val_post_update.sql"),
        provide_context=True,
    )

    validate_data_integrity = PythonOperator(
        task_id="validate_data_integrity_rules",
        python_callable=lambda **_: run_validation("validate_data_integrity.sql"),
        provide_context=True,
    )

    # ------------------------- TASK DEPENDENCIES -----------------------------
    (
        [create_dim_company, create_dim_date, create_dim_symbol]
        >> create_fact_market_daily
        >> [update_dim_company, update_dim_date, update_dim_symbol]
        >> update_fact_market_daily
        >> validate_consistency
        >> validate_data_integrity
    )
