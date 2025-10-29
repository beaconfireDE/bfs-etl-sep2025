from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# ============================================
# DAG 基础参数配置
# ============================================
default_args = {
    "owner": "team2",                   # DAG 归属组（可选，方便区分）
    "depends_on_past": False,           # 不依赖前一次运行结果
    "email_on_failure": False,          # 出错时不发邮件（教学环境可关）
    "retries": 1,                       # 失败时最多重试 1 次
    "retry_delay": timedelta(minutes=3) # 每次重试间隔 3 分钟
}

# ============================================
# 主体 DAG 定义
# ============================================
with DAG(
    dag_id="example_clone_only",                      # DAG 唯一 ID
    description="Clone source tables into COPY layer daily",  # DAG 描述
    default_args=default_args,
    schedule_interval="@daily",                       # 每天调度一次
    start_date=datetime(2025, 10, 29),                # 开始执行日期
    catchup=False,                                    # 不回补历史
    tags=["clone", "snowflake", "team2"],             # UI 分类标签
) as dag:

    # ============= 任务1：克隆 STOCK_HISTORY =============
    clone_stock_history = SnowflakeOperator(
        task_id="clone_stock_history",                # 任务名
        snowflake_conn_id="snowflake_conn",           # 连接 ID（Airflow UI 中配置）
        sql="""
            CREATE OR REPLACE TABLE AIRFLOW0602.DEV.COPY_STOCK_HISTORY_TEAM2
            CLONE US_STOCK_DAILY.DCCM.STOCK_HISTORY;
        """,
    )

    # ============= 任务2：克隆 COMPANY_PROFILE =============
    clone_company_profile = SnowflakeOperator(
        task_id="clone_company_profile",
        snowflake_conn_id="snowflake_conn",
        sql="""
            CREATE OR REPLACE TABLE AIRFLOW0602.DEV.COPY_COMPANY_PROFILE_TEAM2
            CLONE US_STOCK_DAILY.DCCM.COMPANY_PROFILE;
        """,
    )

    # ============= 任务3：克隆 SYMBOLS =============
    clone_symbols = SnowflakeOperator(
        task_id="clone_symbols",
        snowflake_conn_id="snowflake_conn",
        sql="""
            CREATE OR REPLACE TABLE AIRFLOW0602.DEV.COPY_SYMBOLS_TEAM2
            CLONE US_STOCK_DAILY.DCCM.SYMBOLS;
        """,
    )

    # ============= 设置任务依赖（顺序或并行） =============
    # 三个 clone 任务之间没有依赖，可以并行执行
    [clone_stock_history, clone_company_profile, clone_symbols]
