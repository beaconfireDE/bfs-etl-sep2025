from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

SNOWFLAKE_CONN_ID = "snowflake_conn"
DB = "AIRFLOW0928"
SCHEMA = "DEV"

DEFAULT_ARGS = {"owner": "team2",
   "depends_on_past": False,   
     "retries": 1,                # 失败时最多重试 1 次
    "retry_delay": timedelta(minutes=3) # 每次重试间隔 3 分钟
   
   }


with DAG(
    dag_id="team2_stock_validate",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 10, 29),
    schedule="0 6 * * *",  # 每天 06:00 America/Chicago
    catchup=False,
    tags=["stock", "etl", "team2", "unified"],
) as dag:

    # 统一切库&架构（必要）
    use_db_schema = SnowflakeOperator(
        task_id="use_db_schema",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"USE DATABASE {DB}; USE SCHEMA {SCHEMA};",
    )

    # 1) DIM_SYMBOL：Type-1 覆盖（既适合全量也适合增量）
    upsert_dim_symbol = SnowflakeOperator(
        task_id="upsert_dim_symbol",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        MERGE INTO {DB}.{SCHEMA}.DIM_SYMBOL_TEAM2 tgt
        USING (
          SELECT DISTINCT SYMBOL, NAME, EXCHANGE
          FROM {DB}.{SCHEMA}.COPY_SYMBOLS_TEAM2
          WHERE SYMBOL IS NOT NULL
        ) src
        ON tgt.SYMBOL = src.SYMBOL
        WHEN MATCHED THEN UPDATE SET
          NAME = src.NAME,
          EXCHANGE = src.EXCHANGE


        WHEN NOT MATCHED THEN INSERT (SYMBOL, NAME, EXCHANGE)
        VALUES (src.SYMBOL, src.NAME, src.EXCHANGE);
        """,
    )

    # 2) DIM_COMPANY：Type-1 覆盖（以 COPY_COMPANY_PROFILE_TEAM2 最新记录为准）
    upsert_dim_company = SnowflakeOperator(
        task_id="upsert_dim_company",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        MERGE INTO {DB}.{SCHEMA}.DIM_COMPANY_TEAM2 tgt
        USING (
          SELECT
            ds.SYMBOL_ID,
            cp.COMPANYNAME AS COMPANY_NAME,
            cp.INDUSTRY,
            cp.SECTOR,
            cp.CEO,
            cp.WEBSITE,
            cp.DESCRIPTION
          FROM {DB}.{SCHEMA}.COPY_COMPANY_PROFILE_TEAM2 cp
          JOIN {DB}.{SCHEMA}.DIM_SYMBOL_TEAM2 ds
            ON ds.SYMBOL = cp.SYMBOL
          QUALIFY ROW_NUMBER() OVER (PARTITION BY ds.SYMBOL_ID ORDER BY cp.ID DESC) = 1
        ) src
        ON tgt.SYMBOL_ID = src.SYMBOL_ID
        WHEN MATCHED THEN UPDATE SET
          COMPANY_NAME = src.COMPANY_NAME,
          INDUSTRY     = src.INDUSTRY,
          SECTOR       = src.SECTOR,
          CEO          = src.CEO,
          WEBSITE      = src.WEBSITE,
          DESCRIPTION  = src.DESCRIPTION

        WHEN NOT MATCHED THEN INSERT
          (SYMBOL_ID, COMPANY_NAME, INDUSTRY, SECTOR, CEO, WEBSITE, DESCRIPTION)
        VALUES
          (src.SYMBOL_ID, src.COMPANY_NAME, src.INDUSTRY, src.SECTOR, src.CEO, src.WEBSITE, src.DESCRIPTION);
        """,
    )

    # 3) DIM_DATE：自动识别首跑/增量
    #    - 首跑：从 STOCK_HISTORY 的最小日期开始补到最大日期
    #    - 增量：从 DIM_DATE 最大日期+1 补到 STOCK_HISTORY 最大日期
    extend_dim_date = SnowflakeOperator(
        task_id="extend_dim_date",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
INSERT INTO AIRFLOW0928.DEV.DIM_DATE_TEAM2 (DATE_ID, FULL_DATE, DAY, MONTH, QUARTER, YEAR, DAY_OF_WEEK)
WITH src_rng AS (
  SELECT MIN(DATE) AS dmin, MAX(DATE) AS dmax
  FROM AIRFLOW0928.DEV.COPY_STOCK_HISTORY_TEAM2
),
dim_stat AS (
  SELECT COUNT(*) AS cnt, MAX(FULL_DATE) AS dmax_dim
  FROM AIRFLOW0928.DEV.DIM_DATE_TEAM2
),
base AS (
  SELECT
    CASE
      WHEN (SELECT cnt FROM dim_stat) = 0
        THEN (SELECT dmin FROM src_rng)
      ELSE DATEADD(DAY, 1, (SELECT dmax_dim FROM dim_stat))
    END AS start_d,
    (SELECT dmax FROM src_rng) AS end_d
)
, recursive_dates AS (
    SELECT start_d AS d
    FROM base
    UNION ALL
    SELECT DATEADD(DAY, 1, d)
    FROM recursive_dates, base
    WHERE d < base.end_d
)
SELECT
  TO_NUMBER(TO_CHAR(d,'YYYYMMDD')),
  d, DAY(d), MONTH(d), QUARTER(d), YEAR(d), TO_CHAR(d,'DY')
FROM recursive_dates r
LEFT JOIN AIRFLOW0928.DEV.DIM_DATE_TEAM2 dd
  ON dd.FULL_DATE = r.d
WHERE dd.FULL_DATE IS NULL
ORDER BY d;

        """,
    )

    # 4) FACT：首跑/增量合一（自动识别 FACT 是否为空；平时按 FACT 最大日期 - 7 天回补窗口）
    upsert_fact = SnowflakeOperator(
        task_id="upsert_fact",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
-- Step 1: Create a temporary staging table
CREATE OR REPLACE TEMP TABLE TMP_SRC_NEW_TEAM2 AS
WITH fact_stat AS (
  SELECT COUNT(*) AS cnt,
         TO_DATE(TO_CHAR(MAX(DATE_ID)), 'YYYYMMDD') AS fact_max_d
  FROM AIRFLOW0928.DEV.FACT_STOCK_DAILY_TEAM2
),
src_rng AS (
  SELECT MIN(DATE) AS src_min_d, MAX(DATE) AS src_max_d
  FROM AIRFLOW0928.DEV.COPY_STOCK_HISTORY_TEAM2
),
run_span AS (
  SELECT
    CASE
      WHEN (SELECT cnt FROM fact_stat) = 0
        THEN (SELECT src_min_d FROM src_rng)
      ELSE DATEADD(DAY, -7, (SELECT fact_max_d FROM fact_stat))
    END AS start_d,
    (SELECT src_max_d FROM src_rng) AS end_d
),
latest_profile AS (
  SELECT *
  FROM AIRFLOW0928.DEV.COPY_COMPANY_PROFILE_TEAM2
  QUALIFY ROW_NUMBER() OVER (PARTITION BY SYMBOL ORDER BY ID DESC) = 1
)
SELECT
  ds.SYMBOL_ID,
  TO_NUMBER(TO_CHAR(sh.DATE,'YYYYMMDD')) AS DATE_ID,
  sh.OPEN, sh.HIGH, sh.LOW, sh.CLOSE, sh.ADJCLOSE, sh.VOLUME,
  lp.PRICE, lp.BETA, lp.VOLAVG, lp.MKTCAP, lp.DCF, lp.DCFDIFF, lp.CHANGES
FROM AIRFLOW0928.DEV.COPY_STOCK_HISTORY_TEAM2 sh
JOIN run_span r 
  ON sh.DATE BETWEEN r.start_d AND r.end_d
JOIN AIRFLOW0928.DEV.DIM_SYMBOL_TEAM2 ds
  ON ds.SYMBOL = sh.SYMBOL
JOIN AIRFLOW0928.DEV.DIM_DATE_TEAM2 dd
  ON dd.FULL_DATE = sh.DATE
LEFT JOIN latest_profile lp
  ON lp.SYMBOL = sh.SYMBOL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY ds.SYMBOL_ID, TO_NUMBER(TO_CHAR(sh.DATE,'YYYYMMDD'))
  ORDER BY sh.DATE DESC
) = 1;

-- Step 2: Merge into Fact table
MERGE INTO AIRFLOW0928.DEV.FACT_STOCK_DAILY_TEAM2 tgt
USING TMP_SRC_NEW_TEAM2 src
ON tgt.SYMBOL_ID = src.SYMBOL_ID AND tgt.DATE_ID = src.DATE_ID
WHEN MATCHED THEN UPDATE SET
  OPEN=src.OPEN, HIGH=src.HIGH, LOW=src.LOW, CLOSE=src.CLOSE,
  ADJCLOSE=src.ADJCLOSE, VOLUME=src.VOLUME,
  PRICE=src.PRICE, BETA=src.BETA, VOLAVG=src.VOLAVG, MKTCAP=src.MKTCAP,
  DCF=src.DCF, DCFDIFF=src.DCFDIFF, CHANGES=src.CHANGES
WHEN NOT MATCHED THEN INSERT
  (SYMBOL_ID, DATE_ID, OPEN, HIGH, LOW, CLOSE, ADJCLOSE, VOLUME, PRICE, BETA, VOLAVG, MKTCAP, DCF, DCFDIFF, CHANGES)
VALUES
  (src.SYMBOL_ID, src.DATE_ID, src.OPEN, src.HIGH, src.LOW, src.CLOSE, src.ADJCLOSE, src.VOLUME,
   src.PRICE, src.BETA, src.VOLAVG, src.MKTCAP, src.DCF, src.DCFDIFF, src.CHANGES);

        """,
    )

    # Step 5: 数据验证任务 —— 检查维度表、事实表行数和日期范围
def validate_all_tables():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    print("\n==============================")
    print("数据验证报告：")
    print("==============================\n")

    # 1️⃣ DIM_SYMBOL
    cur.execute("""
        SELECT 
          (SELECT COUNT(*) FROM AIRFLOW0928.DEV.COPY_SYMBOLS_TEAM2),
          (SELECT COUNT(DISTINCT SYMBOL) FROM AIRFLOW0928.DEV.COPY_SYMBOLS_TEAM2),
          (SELECT COUNT(*) FROM AIRFLOW0928.DEV.DIM_SYMBOL_TEAM2)
    """)
    copy_total, copy_distinct, dim_rows = cur.fetchone()
    result_symbol = "一致" if copy_distinct == dim_rows else "不一致"
    print(f"[DIM_SYMBOL]\nCOPY总行数: {copy_total}, 去重后: {copy_distinct}, DIM行数: {dim_rows} → {result_symbol}\n")

    # 2️⃣ DIM_COMPANY
    cur.execute("""
        SELECT 
          (SELECT COUNT(*) FROM AIRFLOW0928.DEV.COPY_COMPANY_PROFILE_TEAM2),
          (SELECT COUNT(DISTINCT SYMBOL) FROM AIRFLOW0928.DEV.COPY_COMPANY_PROFILE_TEAM2),
          (SELECT COUNT(*) FROM AIRFLOW0928.DEV.DIM_COMPANY_TEAM2)
    """)
    copy_total, copy_distinct, dim_rows = cur.fetchone()
    result_company = "一致" if copy_distinct == dim_rows else "不一致"
    print(f"[DIM_COMPANY]\nCOPY总行数: {copy_total}, 去重后: {copy_distinct}, DIM行数: {dim_rows} → {result_company}\n")

    # 3️⃣ DIM_DATE
    cur.execute("""
        SELECT 
          (SELECT COUNT(DISTINCT DATE) FROM AIRFLOW0928.DEV.COPY_STOCK_HISTORY_TEAM2),
          (SELECT COUNT(*) FROM AIRFLOW0928.DEV.DIM_DATE_TEAM2),
          (SELECT MAX(DATE) FROM AIRFLOW0928.DEV.COPY_STOCK_HISTORY_TEAM2),
          (SELECT MAX(FULL_DATE) FROM AIRFLOW0928.DEV.DIM_DATE_TEAM2)
    """)
    copy_dates, dim_dates, copy_max, dim_max = cur.fetchone()
    result_date = "日期覆盖完整" if dim_dates >= copy_dates else "日期缺失"
    print(f"[DIM_DATE]\nCOPY唯一日期: {copy_dates}, DIM日期行数: {dim_dates}\n最大日期: COPY={copy_max}, DIM={dim_max} → {result_date}\n")

    # 4️⃣ FACT_STOCK_DAILY
    cur.execute("""
        SELECT
          (SELECT COUNT(*) FROM AIRFLOW0928.DEV.COPY_STOCK_HISTORY_TEAM2),
          (SELECT COUNT(DISTINCT SYMBOL || TO_VARCHAR(DATE)) FROM AIRFLOW0928.DEV.COPY_STOCK_HISTORY_TEAM2),
          (SELECT COUNT(*) FROM AIRFLOW0928.DEV.FACT_STOCK_DAILY_TEAM2)
    """)
    copy_total, copy_distinct, fact_rows = cur.fetchone()
    duplicate_rows = copy_total - copy_distinct
    result_fact = "一致" if fact_rows == copy_distinct else "不一致"
    print(f"[FACT_STOCK_DAILY]\nCOPY总行数: {copy_total}, 去重后: {copy_distinct}, FACT行数: {fact_rows}, 重复行: {duplicate_rows} → {result_fact}\n")

    print("==============================")
    print("🏁 验证完成！")
    print("==============================\n")

    cur.close()
    conn.close()


validate_data = PythonOperator(
    task_id="validate_data",
    python_callable=validate_all_tables,
)





use_db_schema >> upsert_dim_symbol >> upsert_dim_company >> extend_dim_date >> upsert_fact >> validate_data