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
TABLE_DIMCOM = 'dim_company_team1'
TABLE_DIMSYM = 'dim_symbol_team1'
TABLE_FACTPRICE = 'fact_daily_price_team1'

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
		task_id="clone_tables",
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
        task_id="create_dimdate",
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
        task_id="update_dimdate",
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

	create_dimcompany = SnowflakeOperator(
        task_id="create_dimcompany",
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        create or replace table {TABLE_DIMCOM} (
		company_id number(8, 0) primary key identity start 1 increment 1,
		companyname varchar(512) not null,
		website varchar(64),
		description varchar(2048),
		CEO varchar(64),
		sector varchar(64),
		industry varchar(64)
		);
		"""
	)

	update_dimcompany = SnowflakeOperator(
        task_id="update_dimcompany",
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        insert into {TABLE_DIMCOM}
		(companyname, website, description, CEO, sector, industry)

		with ordered_profiles as (
		select coalesce(companyname, 'unnamed company') as companyname, website, description, CEO, sector, industry,
		    row_number() over (partition by companyname order by id) as rn
		from {COPY_TABLE_COM_PROFILE}
		)

		select companyname, website, description, CEO, sector, industry
		from ordered_profiles
		where rn = 1;
		"""
	)

	create_dimsymbol = SnowflakeOperator(
        task_id="create_dimsymbol",
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        create or replace table {TABLE_DIMSYM} (
		symbol varchar(16) primary key,
		exchange varchar(64),
		name varchar(256),
		company_id number(8, 0),
		beta number(18,8),
		volavg number(38, 0),
		mktcap number(38, 0),
		lastdiv number(18,8),
		range varchar(64),
		price number(18,8),
		dcf number(18,8),
		dcfdiff number(18,8),

		constraint fk_dim_symbol_company 
		    foreign key (company_id) references {TABLE_DIMCOM} (company_id)
		);
		"""
	)

	update_dimsymbol = SnowflakeOperator(
        task_id="update_dimsymbol",
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        insert into {TABLE_DIMSYM}
		select s.symbol, s.exchange, s.name,
		    c.company_id, com.beta, com.volavg, com.mktcap, 
		    com.lastdiv, com.range, com.price, com.dcf, com.dcfdiff
		from {COPY_TABLE_SYMBOL} s
		join {COPY_TABLE_COM_PROFILE} com
		on s.symbol = com.symbol
		join {TABLE_DIMCOM} c
		on coalesce(com.companyname, 'unnamed company') = c.companyname;
		"""
	)

	create_factprice = SnowflakeOperator(
		task_id="create_factprice",
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        create or replace table {TABLE_FACTPRICE} (
		price_key number(38, 0) identity,
		symbol varchar(16) not null,
		datekey number(8, 0) not null,
		open number(18,8),
		high number(18,8),
		low number(18,8),
		close number(18,8),
		adjclose number(18,8),
		volume number(38,8),

		constraint fk_daily_date foreign key (datekey) references {TABLE_DIMDATE} (datekey),

		constraint fk_daily_symbol foreign key (symbol) references {TABLE_DIMSYM} (symbol)
		);
		"""
	)

	update_factprice = SnowflakeOperator(
        task_id="update_factprice",
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        insert into {TABLE_FACTPRICE} (symbol, datekey, open, high, low, close, adjclose, volume)
		select h.symbol, d.datekey, h.open, h.high, h.low, h.close, h.adjclose, h.volume
		from {COPY_TABLE_HISTORY} h
		join {TABLE_DIMDATE} d
		on h.date = d.date;
		"""
	)





	clone_tables >> [create_dimdate, create_dimcompany]
	create_dimdate >> update_dimdate
	create_dimcompany >> [create_dimsymbol, update_dimcompany]
	create_dimsymbol >> update_dimsymbol	
	update_dimdate >> update_dimsymbol
	update_dimcompany >> update_dimsymbol
	update_dimsymbol >> create_factprice >> update_factprice

