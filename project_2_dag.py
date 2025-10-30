from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'group_3',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

GROUP_NUM = '3'

with DAG(
    dag_id=f'stock_etl_group_{GROUP_NUM}',
    default_args=default_args,
    start_date=datetime(2025, 10, 29),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # ETL Task 1: Load/Update dim_symbol
    load_dim_symbol = SnowflakeOperator(
        task_id='load_dim_symbol',
        snowflake_conn_id='snowflake_conn',
        sql=f"""
        MERGE INTO AIRFLOW0602.DEV.dim_symbol_{GROUP_NUM} AS target
        USING (
            SELECT DISTINCT symbol, name, exchange
            FROM US_STOCK_DAILY.DCCM.SYMBOLS
        ) AS source
        ON target.symbol = source.symbol
        WHEN MATCHED THEN
            UPDATE SET 
                name = source.name,
                exchange = source.exchange,
                updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (symbol, name, exchange)
            VALUES (source.symbol, source.name, source.exchange);
        """
    )

    # ETL Task 2: Load/Update dim_company (SCD Type 2 - expire old records)
    load_dim_company = SnowflakeOperator(
        task_id='load_dim_company',
        snowflake_conn_id='snowflake_conn',
        sql=f"""
        MERGE INTO AIRFLOW0602.DEV.dim_company_{GROUP_NUM} AS target
        USING (
            SELECT 
                symbol, companyname, sector, industry, exchange, 
                ceo, website, description, CURRENT_DATE() as effective_date
            FROM US_STOCK_DAILY.DCCM.COMPANY_PROFILE
        ) AS source
        ON target.symbol = source.symbol AND target.is_current = TRUE
        WHEN MATCHED AND (
            target.company_name != source.companyname OR
            target.sector != source.sector OR
            target.ceo != source.ceo
        ) THEN
            UPDATE SET 
                is_current = FALSE,
                end_date = CURRENT_DATE(),
                updated_at = CURRENT_TIMESTAMP();
        """
    )

    # ETL Task 3: Insert new version for changed records
    insert_new_version = SnowflakeOperator(
        task_id='insert_new_company_version',
        snowflake_conn_id='snowflake_conn',
        sql=f"""
        INSERT INTO AIRFLOW0602.DEV.dim_company_{GROUP_NUM} 
            (symbol, company_name, sector, industry, exchange, ceo, website, description, effective_date, is_current)
        SELECT 
            cp.symbol, cp.companyname, cp.sector, cp.industry, cp.exchange,
            cp.ceo, cp.website, cp.description, CURRENT_DATE(), TRUE
        FROM US_STOCK_DAILY.DCCM.COMPANY_PROFILE cp
        INNER JOIN AIRFLOW0602.DEV.dim_company_{GROUP_NUM} dc
            ON cp.symbol = dc.symbol AND dc.is_current = FALSE AND dc.end_date = CURRENT_DATE()
        WHERE NOT EXISTS (
            SELECT 1 FROM AIRFLOW0602.DEV.dim_company_{GROUP_NUM} dc2
            WHERE dc2.symbol = cp.symbol AND dc2.is_current = TRUE
        );
        """
    )

    # ETL Task 4: Incremental load fact table
    load_fact_stock = SnowflakeOperator(
        task_id='load_fact_stock',
        snowflake_conn_id='snowflake_conn',
        sql=f"""
        MERGE INTO AIRFLOW0602.DEV.fact_stock_daily_{GROUP_NUM} AS target
        USING (
            SELECT 
                dc.company_key, ds.symbol_key, sh.date,
                sh.open, sh.high, sh.low, sh.close, sh.adjclose, sh.volume,
                cp.price, cp.changes, cp.beta, cp.volavg, cp.mktcap, cp.lastdiv
            FROM US_STOCK_DAILY.DCCM.STOCK_HISTORY sh
            LEFT JOIN US_STOCK_DAILY.DCCM.COMPANY_PROFILE cp ON sh.symbol = cp.symbol
            LEFT JOIN AIRFLOW0602.DEV.dim_symbol_{GROUP_NUM} ds ON sh.symbol = ds.symbol
            LEFT JOIN AIRFLOW0602.DEV.dim_company_{GROUP_NUM} dc 
                ON sh.symbol = dc.symbol AND dc.is_current = TRUE
            WHERE sh.date >= DATEADD(day, -7, CURRENT_DATE())
        ) AS source
        ON target.symbol_key = source.symbol_key AND target.date = source.date
        WHEN MATCHED THEN
            UPDATE SET open = source.open, high = source.high, low = source.low,
                      close = source.close, volume = source.volume, price = source.price
        WHEN NOT MATCHED THEN
            INSERT (company_key, symbol_key, date, open, high, low, close, adjclose, volume,
                    price, changes, beta, volavg, mktcap, lastdiv)
            VALUES (source.company_key, source.symbol_key, source.date, source.open, 
                    source.high, source.low, source.close, source.adjclose, source.volume,
                    source.price, source.changes, source.beta, source.volavg, 
                    source.mktcap, source.lastdiv);
        """
    )

    # Task dependencies
    [load_dim_symbol, load_dim_company] >> insert_new_version >> load_fact_stock