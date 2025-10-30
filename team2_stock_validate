from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = "snowflake_conn"
DB = "AIRFLOW0928"
SCHEMA = "DEV"

DEFAULT_ARGS = {"owner": "team2",
   "depends_on_past": False,   
     "retries": 1,                # 失败时最多重试 1 次
    "retry_delay": timedelta(minutes=3) # 每次重试间隔 3 分钟
   
   }


with DAG(
    dag_id="stock_etl_unified_team2",
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
validate_data = SnowflakeOperator(
    task_id="validate_data",
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    -- 返回关键表行数和日期范围，用于快速校验
    SELECT
      (SELECT COUNT(*) FROM {DB}.{SCHEMA}.DIM_SYMBOL_TEAM2)   AS dim_symbol_rows,
      (SELECT COUNT(*) FROM {DB}.{SCHEMA}.DIM_COMPANY_TEAM2)  AS dim_company_rows,
      (SELECT COUNT(*) FROM {DB}.{SCHEMA}.DIM_DATE_TEAM2)     AS dim_date_rows,
      (SELECT COUNT(*) FROM {DB}.{SCHEMA}.FACT_STOCK_DAILY_TEAM2) AS fact_rows,
      (SELECT MAX(FULL_DATE) FROM {DB}.{SCHEMA}.DIM_DATE_TEAM2)   AS latest_dim_date,
      (SELECT MAX(DATE) FROM {DB}.{SCHEMA}.COPY_STOCK_HISTORY_TEAM2) AS latest_src_date;
    """,
)


    use_db_schema >> upsert_dim_symbol >> upsert_dim_company >> extend_dim_date >> upsert_fact >> validate_data