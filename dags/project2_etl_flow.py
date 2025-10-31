from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

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


'''Define function to run sanity checks later'''
def run_dq_checks_log_only():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sqls = {
        "dup_fact_key": """
            select count(*) 
            from (
              select symbol, datekey, count(*) as c
              from fact_daily_price_team1
              group by 1,2
              having count(*) > 1
            )
        """,
        "orphan_date": """
            select count(*)
            from fact_daily_price_team1 f
            left join dim_date_team1 d on d.datekey = f.datekey
            where d.datekey is null
        """,
        "orphan_symbol": """
            select count(*)
            from fact_daily_price_team1 f
            left join dim_symbol_team1 s on s.symbol = f.symbol
            where s.symbol is null
        """,
        "ohlc_sanity": """
            select count(*)
            from fact_daily_price_team1
            where volume < 0
               or low   > least(open, close)
               or high  < greatest(open, close)
               or low   > high
        """,
    }

    results = {}
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for name, q in sqls.items():
                cur.execute(q)
                cnt = cur.fetchone()[0]
                results[name] = cnt

    # log results 
    print("=== Data Quality Check Results ===")
    for name, cnt in results.items():
        print(f"{name}: {cnt} issues found")

    
    return results

# Setting up DAG
with DAG(
	dag_id="project2_etl_taskflow",
    start_date=datetime(2025, 10, 24),
    schedule='0 5,10,15 * * *', # Run at 5:00, 10:00,and 15:00
    # schedule = None, # Run on demand
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
        create table if not exists {TABLE_DIMDATE} (
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

		merge into {TABLE_DIMDATE} tgt
		using (
		  with recursive more_dates as (
		    select $min_date as d
		    union all
		    select dateadd('day', 1, d) from more_dates where d < $max_date
		  )
		  select d as date,
		         cast(to_char(d,'YYYYMMDD') as number) as datekey,
		         year(d) as year, quarter(d) as quarter, month(d) as month,
		         to_char(d,'MMMM') as month_name, day(d) as day,
		         dayofweekiso(d) as day_of_week, to_char(d,'DY') as dow_name,
		         weekofyear(d) as week_of_year,
		         iff(dayofweekiso(d) >= 6, True, False) as is_weekend
		  from more_dates
		) src
		on tgt.date = src.date
		when matched then update set
		  tgt.datekey = src.datekey,
		  tgt.year = src.year,
		  tgt.quarter = src.quarter,
		  tgt.month = src.month,
		  tgt.month_name = src.month_name,
		  tgt.day = src.day,
		  tgt.day_of_week = src.day_of_week,
		  tgt.dow_name = src.dow_name,
		  tgt.week_of_year = src.week_of_year,
		  tgt.is_weekend = src.is_weekend
		when not matched then insert (date, datekey, year, quarter, month, month_name, day, day_of_week, dow_name, week_of_year, is_weekend)
		values (src.date, src.datekey, src.year, src.quarter, src.month, src.month_name, src.day, src.day_of_week, src.dow_name, src.week_of_year, src.is_weekend);

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
        create table if not exists {TABLE_DIMCOM} (
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
        merge into {TABLE_DIMCOM} tgt
		using (
		  with ordered_profiles as (
		    select coalesce(companyname,'unnamed company') as companyname, website, description, CEO, sector, industry,
		           row_number() over (partition by companyname order by id) as rn
		    from {COPY_TABLE_COM_PROFILE}
		  )
		  select * from ordered_profiles where rn = 1
		) src
		on tgt.companyname = src.companyname
		when matched then update set
		  tgt.website = src.website,
		  tgt.description = src.description,
		  tgt.CEO = src.CEO,
		  tgt.sector = src.sector,
		  tgt.industry = src.industry
		when not matched then insert (companyname, website, description, CEO, sector, industry)
		values (src.companyname, src.website, src.description, src.CEO, src.sector, src.industry);

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
        create table if not exists {TABLE_DIMSYM} (
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
        merge into {TABLE_DIMSYM} tgt
		using (
		  select s.symbol, s.exchange, s.name,
		         c.company_id, com.beta, com.volavg, com.mktcap,
		         com.lastdiv, com.range, com.price, com.dcf, com.dcfdiff
		  from {COPY_TABLE_SYMBOL} s
		  join {COPY_TABLE_COM_PROFILE} com on s.symbol = com.symbol
		  join {TABLE_DIMCOM} c on coalesce(com.companyname,'unnamed company') = c.companyname
		) src
		on tgt.symbol = src.symbol
		when matched then update set
		  tgt.exchange = src.exchange,
		  tgt.name = src.name,
		  tgt.company_id = src.company_id,
		  tgt.beta = src.beta,
		  tgt.volavg = src.volavg,
		  tgt.mktcap = src.mktcap,
		  tgt.lastdiv = src.lastdiv,
		  tgt.range = src.range,
		  tgt.price = src.price,
		  tgt.dcf = src.dcf,
		  tgt.dcfdiff = src.dcfdiff
		when not matched then insert (symbol, exchange, name, company_id, beta, volavg, mktcap, lastdiv, range, price, dcf, dcfdiff)
		values (src.symbol, src.exchange, src.name, src.company_id, src.beta, src.volavg, src.mktcap, src.lastdiv, src.range, src.price, src.dcf, src.dcfdiff);

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
        create table if not exists {TABLE_FACTPRICE} (
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
        merge into {TABLE_FACTPRICE} tgt
		using (

			with rownum as (
		    select symbol, date, open, high, low, close, adjclose, volume,
		        row_number() over (partition by symbol, date order by symbol) as rn
		    from {COPY_TABLE_HISTORY}
		),
		unique_price as (
		select symbol, date, open, high, low, close, adjclose, volume from rownum
		where rn = 1
		)
		select h.symbol, d.datekey, h.open, h.high, h.low, h.close, h.adjclose, h.volume
		from unique_price h
		join {TABLE_DIMDATE} d
		on h.date = d.date
		) src

		on tgt.symbol = src.symbol and tgt.datekey = src.datekey
		when matched then update set
		  tgt.open = src.open,
		  tgt.high = src.high,
		  tgt.low = src.low,
		  tgt.close = src.close,
		  tgt.adjclose = src.adjclose,
		  tgt.volume = src.volume
		when not matched then insert (symbol, datekey, open, high, low, close, adjclose, volume)
		values (src.symbol, src.datekey, src.open, src.high, src.low, src.close, src.adjclose, src.volume);

		"""
	)

	# sanity_duplicates = SnowflakeOperator(
	# 	task_id="sanity_duplicates",
    #     warehouse=SNOWFLAKE_WAREHOUSE,
    #     database=SNOWFLAKE_DATABASE,
    #     schema=SNOWFLAKE_SCHEMA,
    #     role=SNOWFLAKE_ROLE,
    #     snowflake_conn_id=SNOWFLAKE_CONN_ID,
    #     sql=f"""
    #     select count(*) as num_duplicates
	# 	from
	# 	(select symbol, datekey, count(*) as cnt
	# 	from {TABLE_FACTPRICE}
	# 	group by symbol, datekey
	# 	having count(*) > 1);
	# 	"""
	# )

	# sanity_miss_date = SnowflakeOperator(
	# 	task_id="sanity_miss_date",
    #     warehouse=SNOWFLAKE_WAREHOUSE,
    #     database=SNOWFLAKE_DATABASE,
    #     schema=SNOWFLAKE_SCHEMA,
    #     role=SNOWFLAKE_ROLE,
    #     snowflake_conn_id=SNOWFLAKE_CONN_ID,
    #     sql=f"""
    #     select count(*) as num_missing_dates
	# 	from
	# 	(select f.symbol, f.datekey
	# 	from {TABLE_FACTPRICE} f
	# 	left join {TABLE_DIMDATE} d on d.datekey = f.datekey
	# 	where d.datekey is null);
	# 	"""
	# )	

	# sanity_miss_symbol = SnowflakeOperator(
	# 	task_id="sanity_miss_symbol",
    #     warehouse=SNOWFLAKE_WAREHOUSE,
    #     database=SNOWFLAKE_DATABASE,
    #     schema=SNOWFLAKE_SCHEMA,
    #     role=SNOWFLAKE_ROLE,
    #     snowflake_conn_id=SNOWFLAKE_CONN_ID,
    #     sql=f"""
    #     select count(*) as num_missing_symbols
	# 	from
	# 	(select f.symbol, f.datekey
	# 	from {TABLE_FACTPRICE} f
	# 	left join {TABLE_DIMSYM} s on s.symbol = f.symbol
	# 	where s.symbol is null);
	# 	"""
	# )	

	dq_checks = PythonOperator(
	    task_id="dq_checks_log_only",
	    python_callable=run_dq_checks_log_only,
	)






	clone_tables >> [create_dimdate, create_dimcompany]
	create_dimdate >> update_dimdate
	create_dimcompany >> [create_dimsymbol, update_dimcompany]
	create_dimsymbol >> update_dimsymbol	
	update_dimcompany >> update_dimsymbol
	[create_dimdate, create_dimsymbol] >> create_factprice
	[update_dimdate, update_dimsymbol] >> update_factprice
	# update_factprice >> [sanity_duplicates, sanity_miss_date, sanity_miss_symbol]
	update_factprice >> dq_checks
