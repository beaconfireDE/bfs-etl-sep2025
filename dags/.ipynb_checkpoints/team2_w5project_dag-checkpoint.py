from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


# DAG 基础参数配置

default_args = {
    "owner": "team2",                   # DAG 归属的tag
    "depends_on_past": False,          
    "email_on_failure": False,          
    "retries": 1,                       # 失败时最多重试 1 次
    "retry_delay": timedelta(minutes=3) # 每次重试间隔 3 分钟
}

# ============================================
# 主体 DAG 定义
# ============================================
with DAG(
    dag_id="example_clone_only",                      # DAG ID
    description="Clone source tables into COPY layer daily",  
    default_args=default_args,
    schedule_interval="@daily",                       # 每天
    start_date=datetime(2025, 10, 29),                # 开始日期
    catchup=False,                                    
    tags=["clone", "snowflake", "team2"],             
) as dag:

    #clone STOCK_HISTORY 
    clone_stock_history = SnowflakeOperator(
        task_id="clone_stock_history",                
        snowflake_conn_id="snowflake_conn",           
        sql="""
            CREATE OR REPLACE TABLE AIRFLOW0928.DEV.COPY_STOCK_HISTORY_TEAM2
            CLONE US_STOCK_DAILY.DCCM.STOCK_HISTORY;
        """,
    )

    # clone COMPANY_PROFILE 
    clone_company_profile = SnowflakeOperator(
        task_id="clone_company_profile",
        snowflake_conn_id="snowflake_conn",
        sql="""
            CREATE OR REPLACE TABLE AIRFLOW0928.DEV.COPY_COMPANY_PROFILE_TEAM2
            CLONE US_STOCK_DAILY.DCCM.COMPANY_PROFILE;
        """,
    )

    # clone SYMBOLS 
    clone_symbols = SnowflakeOperator(
        task_id="clone_symbols",
        snowflake_conn_id="snowflake_conn",
        sql="""
            CREATE OR REPLACE TABLE AIRFLOW0928.DEV.COPY_SYMBOLS_TEAM2
            CLONE US_STOCK_DAILY.DCCM.SYMBOLS;
        """,
    )


