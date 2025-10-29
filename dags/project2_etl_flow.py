from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

# Adding configs
SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW0928'
SNOWFLAKE_SCHEMA = 'DEV'

SNOWFLAKE_ROLE = 'DE_DEVELOPER_0928'
SNOWFLAKE_WAREHOUSE = 'DE_0928_WH'
# SNOWFLAKE_STAGE = 's3_stage_trans_order'
# SNOWFLAKE_FORMAT = 'assignment0928.as_prod.team1_prj1_format'
SOURCE_TABLE_COM_PROFILE = 'US_STOCK_DAILY.DCCM.Company_Profile'
SOURCE_TABLE_HISTORY = 'US_STOCK_DAILY.DCCM.Stock_History'
SOURCE_TABLE_SYMBOL = 'US_STOCK_DAILY.DCCM.Symbols'


COPY_TABLE_COM_PROFILE = 'copy_company_profile_team1'
COPY_TABLE_HISTORY = 'copy_stock_hist_team1'
COPY_TABLE_SYMBOL = 'copy_stock_team1'

TABLE_DIMDATE = 'dim_date_team1'

# Setting up DAG
with DAG(
	dag_id="project2_etl_taskflow",
    start_date=datetime(2025, 10, 24),
    # schedule='0 7 * * *',
    schedule = None, # Run on demand
    # default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['project2', 'snowflake', 'S3'],
    catchup=False,
) as dag:
	clone_tables = SnowflakeOperator(
		task_id="snowflake_clone_tables",
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        create table if not exists {COPY_TABLE_COM_PROFILE}
		clone {SOURCE_TABLE_COM_PROFILE};

		create table if not exists {COPY_TABLE_HISTORY}
		clone {SOURCE_TABLE_HISTORY};

		create table if not exists {COPY_TABLE_SYMBOL}
		clone {SOURCE_TABLE_SYMBOL};
		"""
	)
	create_dimdate = SnowflakeOperator(
        task_id="snowflake_create_dimdate",
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        create or replace table {TABLE_DIMDATE} (
		datekey number(8, 0) primary key,
		date date not null unique, 
		year number(4, 0),
		quarter number(1, 0),
		month number(2, 0),
		month_name varchar,
		day number(2, 0),
		day_of_week number(1, 0),
		dow_name varchar,
		week_of_year number,
		is_weekend boolean
		);
		"""
	)

	update_dimdate = SnowflakeOperator(
        task_id="snowflake_update_dimdate",
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        set min_date = (select min(date) from {COPY_TABLE_HISTORY});
		set max_date = current_date(); 

		insert into {TABLE_DIMDATE} 
		(date, datekey, year, quarter, month, month_name, day, day_of_week, dow_name, week_of_year, is_weekend)
		with recursive more_dates as (
		    select $min_date as d
		    union all
		    select dateadd('day', 1, d)
		    from more_dates
		    where d < $max_date
		)
		select 
		    d,
		    cast(to_char(d, 'YYYYMMDD') as number),
		    year(d),
		    quarter(d),
		    month(d),
		    to_char(d, 'MMMM'),
		    day(d),
		    dayofweekiso(d),
		    to_char(d, 'DY'),
		    weekofyear(d),
		    iff(dayofweekiso(d) >= 6, True, False)
		from more_dates;
		"""
	)


	clone_tables >> create_dimdate >> update_dimdate


