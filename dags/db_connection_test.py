from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

SNOWFLAKE_CONN_ID = "snowflake_conn"
SNOWFLAKE_DATABASE = 'AIRFLOW0928'
SNOWFLAKE_SCHEMA = 'DEV'

SNOWFLAKE_ROLE = 'DE_DEVELOPER_0928'
SNOWFLAKE_WAREHOUSE = 'DE_0928_WH'

with DAG(
    dag_id="test_snowflake_connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "snowflake"],
) as dag:

    test_conn = SnowflakeOperator(
        task_id="test_connection",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
        SELECT CURRENT_ACCOUNT() AS account,
               CURRENT_ROLE() AS role,
               CURRENT_DATABASE() AS database_name,
               CURRENT_SCHEMA() AS schema_name,
               CURRENT_WAREHOUSE() AS warehouse;
        """,
    )

    test_table = SnowflakeOperator(
        task_id="test_create_drop",
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
        CREATE OR REPLACE TABLE TEST_CONN_TABLE (id INT);
        INSERT INTO TEST_CONN_TABLE VALUES (1);
        SELECT COUNT(*) AS rowcount FROM TEST_CONN_TABLE;
        DROP TABLE TEST_CONN_TABLE;
        """,
    )

    test_conn >> test_table
