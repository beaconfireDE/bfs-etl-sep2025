from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.empty import EmptyOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW0928'
SNOWFLAKE_SCHEMA = 'DEV'

SNOWFLAKE_ROLE = 'DE_DEVELOPER_0928'
SNOWFLAKE_WAREHOUSE = 'DE_0928_WH'
SNOWFLAKE_STAGE = 's3_stage_trans_order'
SNOWFLAKE_FORMAT = 'assignment0928.as_prod.team1_prj1_format'
TABLE_NAME = 'prestage_orders_team1'

with DAG(
	dag_id="project1_s3_data_transfer",
    start_date=datetime(2025, 10, 24),
    end_date=datetime(2025, 10, 26),
    # schedule='0 7 * * *',
    schedule = None, # Run on demand
    # default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['project1', 'snowflake', 'S3'],
    catchup=True,
) as dag:
	
	start = EmptyOperator(task_id="start")

	setup_format = SnowflakeOperator(
        task_id="snowflake_config_format",
        snowflake_conn_id={SNOWFLAKE_CONN_ID},
        sql=f"""
        use warehouse {SNOWFLAKE_WAREHOUSE};

		use database {SNOWFLAKE_DATABASE};
		use schema {SNOWFLAKE_SCHEMA};

		create or replace file format {SNOWFLAKE_FORMAT}
		  TYPE = CSV
		  field_delimiter = ','
		  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
		  SKIP_HEADER = 1
		  NULL_IF = ('', 'NULL');
		"""
	)

	setup_table = SnowflakeOperator(
        task_id="snowflake_config_table",
        snowflake_conn_id={SNOWFLAKE_CONN_ID},
        sql=f"""
        use warehouse {SNOWFLAKE_WAREHOUSE};

		use database {SNOWFLAKE_DATABASE};
		use schema {SNOWFLAKE_SCHEMA};

		create or replace table {TABLE_NAME} (
		  order_id        NUMBER,
		  customer_id     NUMBER,
		  order_date      DATE,
		  product_id      NUMBER,
		  product_name    STRING,
		  qty             NUMBER,
		  price           NUMBER(10,2),
		  currency        STRING,
		  sales_channel   STRING,
		  region          STRING
		);
		"""
	)

	copy_table = SnowflakeOperator(
        task_id="snowflake_data_transfer",
        snowflake_conn_id={SNOWFLAKE_CONN_ID},
        sql=f"""
        use warehouse {SNOWFLAKE_WAREHOUSE};

		use database {SNOWFLAKE_DATABASE};
		use schema {SNOWFLAKE_SCHEMA};

		COPY INTO {TABLE_NAME}
		FROM @{SNOWFLAKE_STAGE}
		PATTERN = '.*team1_.*\.csv'
		FILE_FORMAT = {SNOWFLAKE_FORMAT}
		ON_ERROR = 'CONTINUE';
		"""
	)

	result_check = SnowflakeOperator(
        task_id="snowflake_data_transfer",
        snowflake_conn_id={SNOWFLAKE_CONN_ID},
        sql=f"""
        use warehouse {SNOWFLAKE_WAREHOUSE};

		use database {SNOWFLAKE_DATABASE};
		use schema {SNOWFLAKE_SCHEMA};

		select count(*) as total_rows
		from {TABLE_NAME}
		"""
	)

	start >> [setup_format, setup_table]
	setup_format >> copy_table
	setup_table >> copy_table
	copy_table >> result_check