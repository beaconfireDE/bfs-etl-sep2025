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
     "retries": 1,                # Retry once when failed
    "retry_delay": timedelta(minutes=3) # Every retry is seperated in 3 minutes
   
   }


with DAG(
    dag_id="team2_stock_validate_test",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 10, 29),
    schedule="0 6 * * *",  # Start at 06:00 America/Chicago
    catchup=False,
    tags=["stock", "etl", "team2", "unified"],
) as dag:




    # Unify Database, Schema and Connection
    use_db_schema = SnowflakeOperator(
        task_id="use_db_schema",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"USE DATABASE {DB}; USE SCHEMA {SCHEMA};",
    )


    # 1) Clone Tables 

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



    # 2) DIM_SYMBOL：Type-1 Coverage (suitable for both full and incremental)
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

    # 3) DIM_COMPANY：Type-1 Overwrite (based on the latest record in COPY_COMPANY_PROFILE_TEAM2)
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

    # 4) DIM_DATE：Automatically identify initial run/incremental
      # - Initial run: Supplement from the minimum date in STOCK_HISTORY to the maximum date# 
      # - Incremental: Supplement from the maximum date in DIM_DATE +1 to the maximum date in STOCK_HISTORY
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

    # 5) FACT: Initial run/incremental integration (automatically detects whether FACT is empty; normally backfills using FACT's maximum date minus a 7-day window)
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

    # 6) Ensure Results Table
    ensure_results_table = SnowflakeOperator(
       task_id="ensure_results_table",
       snowflake_conn_id=SNOWFLAKE_CONN_ID,
       sql=f"""
       CREATE TABLE IF NOT EXISTS {DB}.{SCHEMA}.DQ_RESULTS (
       check_time   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
       check_name   STRING,
       status       STRING,  -- PASSED / FAILED / WARN
       details      STRING,
       failed_count NUMBER
       );
       """,
       )

    # 7) Validate Data Integrity
    validate_data_sql = SnowflakeOperator(
    task_id="validate_integrity_sql",
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    -- 1) DIM_SYMBOL：PK 唯一、symbol 唯一、非空
    INSERT INTO {DB}.{SCHEMA}.DQ_RESULTS (check_name, status, details, failed_count)
    SELECT 'DIM_SYMBOL_PK_UNIQUE',
           IFF(COUNT(*) = COUNT(DISTINCT symbol_id), 'PASSED', 'FAILED'),
           'symbol_id must be unique',
           ABS(COUNT(*) - COUNT(DISTINCT symbol_id))
    FROM {DB}.{SCHEMA}.DIM_SYMBOL_TEAM2;

    INSERT INTO {DB}.{SCHEMA}.DQ_RESULTS (check_name, status, details, failed_count)
    SELECT 'DIM_SYMBOL_SYMBOL_UNIQUE',
           IFF(COUNT(*) = COUNT(DISTINCT symbol), 'PASSED', 'FAILED'),
           'symbol must be unique',
           ABS(COUNT(*) - COUNT(DISTINCT symbol))
    FROM {DB}.{SCHEMA}.DIM_SYMBOL_TEAM2;

    INSERT INTO {DB}.{SCHEMA}.DQ_RESULTS (check_name, status, details, failed_count)
    SELECT 'DIM_SYMBOL_NOT_NULL',
           IFF(COUNT(*)=0,'PASSED','FAILED'),
           'symbol must be NON-NULL/NON-EMPTY',
           COUNT(*)
    FROM {DB}.{SCHEMA}.DIM_SYMBOL_TEAM2
    WHERE symbol IS NULL OR TRIM(symbol) = '';

    -- 2) DIM_DATE：键一致、自然日连续
    INSERT INTO {DB}.{SCHEMA}.DQ_RESULTS (check_name, status, details, failed_count)
    SELECT 'DIM_DATE_KEY_MATCH',
           IFF(COUNT(*)=0,'PASSED','FAILED'),
           'date_id must equal TO_CHAR(full_date,''YYYYMMDD'')',
           COUNT(*)
    FROM {DB}.{SCHEMA}.DIM_DATE_TEAM2
    WHERE date_id <> TO_NUMBER(TO_CHAR(full_date,'YYYYMMDD'));

    WITH d AS (
      SELECT full_date, LAG(full_date) OVER (ORDER BY full_date) AS prev_d
      FROM {DB}.{SCHEMA}.DIM_DATE_TEAM2
    )
    INSERT INTO {DB}.{SCHEMA}.DQ_RESULTS (check_name, status, details, failed_count)
    SELECT 'DIM_DATE_GAPS',
           IFF(COUNT(*)=0,'PASSED','FAILED'),
           'natural day gaps detected',
           COUNT(*)
    FROM d
    WHERE prev_d IS NOT NULL AND DATEDIFF('day', prev_d, full_date) <> 1;

    -- 3) DIM_COMPANY 外键
    INSERT INTO {DB}.{SCHEMA}.DQ_RESULTS (check_name, status, details, failed_count)
    SELECT 'DIM_COMPANY_SYMBOL_FK',
           IFF(COUNT(*)=0,'PASSED','FAILED'),
           'DIM_COMPANY.symbol_id must exist in DIM_SYMBOL',
           COUNT(*)
    FROM {DB}.{SCHEMA}.DIM_COMPANY_TEAM2 c
    LEFT JOIN {DB}.{SCHEMA}.DIM_SYMBOL_TEAM2 s
      ON s.symbol_id = c.symbol_id
    WHERE s.symbol_id IS NULL;

    -- 4) FACT：FK、复合键唯一、非空、非负、OHLC 关系
    INSERT INTO {DB}.{SCHEMA}.DQ_RESULTS (check_name, status, details, failed_count)
    SELECT 'FACT_SYMBOL_FK',
           IFF(COUNT(*)=0,'PASSED','FAILED'),
           'FACT.symbol_id must exist in DIM_SYMBOL',
           COUNT(*)
    FROM {DB}.{SCHEMA}.FACT_STOCK_DAILY_TEAM2 f
    LEFT JOIN {DB}.{SCHEMA}.DIM_SYMBOL_TEAM2 s
      ON s.symbol_id = f.symbol_id
    WHERE s.symbol_id IS NULL;

    INSERT INTO {DB}.{SCHEMA}.DQ_RESULTS (check_name, status, details, failed_count)
    SELECT 'FACT_DATE_FK',
           IFF(COUNT(*)=0,'PASSED','FAILED'),
           'FACT.date_id must exist in DIM_DATE',
           COUNT(*)
    FROM {DB}.{SCHEMA}.FACT_STOCK_DAILY_TEAM2 f
    LEFT JOIN {DB}.{SCHEMA}.DIM_DATE_TEAM2 d
      ON d.date_id = f.date_id
    WHERE d.date_id IS NULL;

    INSERT INTO {DB}.{SCHEMA}.DQ_RESULTS (check_name, status, details, failed_count)
    SELECT 'FACT_UNIQUENESS_BY_KEY',
           IFF(COUNT(*)=0,'PASSED','FAILED'),
           'FACT must be unique by (symbol_id, date_id)',
           COUNT(*)
    FROM (
      SELECT symbol_id, date_id
      FROM {DB}.{SCHEMA}.FACT_STOCK_DAILY_TEAM2
      GROUP BY 1,2
      HAVING COUNT(*) > 1
    );

    INSERT INTO {DB}.{SCHEMA}.DQ_RESULTS (check_name, status, details, failed_count)
    SELECT 'FACT_NOT_NULLS',
           IFF(COUNT(*)=0,'PASSED','FAILED'),
           'key/measures must be NOT NULL',
           COUNT(*)
    FROM {DB}.{SCHEMA}.FACT_STOCK_DAILY_TEAM2
    WHERE symbol_id IS NULL OR date_id IS NULL
       OR open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL OR adjclose IS NULL;

    INSERT INTO {DB}.{SCHEMA}.DQ_RESULTS (check_name, status, details, failed_count)
    SELECT 'FACT_NON_NEGATIVE',
           IFF(COUNT(*)=0,'PASSED','FAILED'),
           'open/high/low/close/adjclose/volume must be >= 0',
           COUNT(*)
    FROM {DB}.{SCHEMA}.FACT_STOCK_DAILY_TEAM2
    WHERE open < 0 OR high < 0 OR low < 0 OR close < 0 OR adjclose < 0 OR volume < 0;

    INSERT INTO {DB}.{SCHEMA}.DQ_RESULTS (check_name, status, details, failed_count)
    SELECT 'FACT_OHLC_CONSISTENCY',
           IFF(COUNT(*)=0,'PASSED','FAILED'),
           'high >= GREATEST(open,close) AND low <= LEAST(open,close)',
           COUNT(*)
    FROM {DB}.{SCHEMA}.FACT_STOCK_DAILY_TEAM2
    WHERE high < GREATEST(open, close) OR low > LEAST(open, close);

    -- 5) 源↔目标：最近 30 天 键行数一致
    WITH mx AS (SELECT MAX(date) AS mx FROM {DB}.{SCHEMA}.COPY_STOCK_HISTORY_TEAM2),
    src AS (
      SELECT symbol, TO_NUMBER(TO_CHAR(date,'YYYYMMDD')) AS date_id, COUNT(*) c
      FROM {DB}.{SCHEMA}.COPY_STOCK_HISTORY_TEAM2
      WHERE date >= DATEADD(day, -30, (SELECT mx FROM mx))
      GROUP BY 1,2
    ),
    tgt AS (
      SELECT s.symbol, f.date_id, COUNT(*) c
      FROM {DB}.{SCHEMA}.FACT_STOCK_DAILY_TEAM2 f
      JOIN {DB}.{SCHEMA}.DIM_SYMBOL_TEAM2 s ON s.symbol_id = f.symbol_id
      WHERE TO_DATE(TO_CHAR(f.date_id),'YYYYMMDD') >= DATEADD(day, -30, (SELECT mx FROM mx))
      GROUP BY 1,2
    ),
    mm AS (
      SELECT COALESCE(src.symbol,tgt.symbol) AS symbol,
             COALESCE(src.date_id,tgt.date_id) AS date_id,
             NVL(src.c,0) AS c_src,
             NVL(tgt.c,0) AS c_tgt
      FROM src FULL OUTER JOIN tgt USING(symbol,date_id)
    )
    INSERT INTO {DB}.{SCHEMA}.DQ_RESULTS (check_name, status, details, failed_count)
    SELECT 'SRC_TGT_ROWCOUNT_30D',
           IFF(SUM(IFF(c_src<>c_tgt,1,0))=0,'PASSED','FAILED'),
           'rowcount by (symbol,date) in last 30 days',
           SUM(IFF(c_src<>c_tgt,1,0))
    FROM mm;

    -- 6) 价格对账：最近 7 天 close 差异
    WITH mx AS (SELECT MAX(date) AS mx FROM {DB}.{SCHEMA}.COPY_STOCK_HISTORY_TEAM2),
    j AS (
      SELECT sh.symbol,
             TO_NUMBER(TO_CHAR(sh.date,'YYYYMMDD')) AS date_id,
             sh.close AS src_close,
             f.close  AS tgt_close
      FROM {DB}.{SCHEMA}.COPY_STOCK_HISTORY_TEAM2 sh
      JOIN {DB}.{SCHEMA}.DIM_SYMBOL_TEAM2 s ON s.symbol = sh.symbol
      JOIN {DB}.{SCHEMA}.FACT_STOCK_DAILY_TEAM2 f
        ON f.symbol_id = s.symbol_id
       AND f.date_id   = TO_NUMBER(TO_CHAR(sh.date,'YYYYMMDD'))
      WHERE sh.date >= DATEADD(day, -7, (SELECT mx FROM mx))
    )
    INSERT INTO {DB}.{SCHEMA}.DQ_RESULTS (check_name, status, details, failed_count)
    SELECT 'SRC_TGT_CLOSE_MATCH_7D',
           IFF(COUNT_IF(ABS(src_close - tgt_close) > 1e-6)=0,'PASSED','WARN'),
           'abs(src.close - fact.close) <= 1e-6 in last 7 days',
           COUNT_IF(ABS(src_close - tgt_close) > 1e-6)
    FROM j;

    -- 7) 回补覆盖：FACT 最大日往回 7 天，自然日每天至少有 1 行
    WITH mx AS (SELECT MAX(date_id) AS mx FROM {DB}.{SCHEMA}.FACT_STOCK_DAILY_TEAM2),
    rng AS (SELECT TO_DATE(TO_CHAR(mx),'YYYYMMDD') AS mx_d FROM mx),
    span AS (SELECT DATEADD(day, -7, mx_d) AS start_d, mx_d AS end_d FROM rng),
    cal AS (
      SELECT DATEADD(day, ROW_NUMBER() OVER (ORDER BY SEQ4())-1, start_d) AS d
      FROM span, TABLE(GENERATOR(ROWCOUNT => 400))
      QUALIFY d <= end_d
    ),
    miss AS (
      SELECT c.d
      FROM cal c
      LEFT JOIN {DB}.{SCHEMA}.FACT_STOCK_DAILY_TEAM2 f
        ON f.date_id = TO_NUMBER(TO_CHAR(c.d,'YYYYMMDD'))
      GROUP BY c.d
      HAVING COUNT(f.fact_id) = 0
    )
    INSERT INTO {DB}.{SCHEMA}.DQ_RESULTS (check_name, status, details, failed_count)
    SELECT 'FACT_COVERAGE_LAST_7D',
           IFF(COUNT(*)=0,'PASSED','FAILED'),
           'at least 1 row per natural day in last 7 days',
           COUNT(*)
    FROM miss;

    -- 8) DIM_COMPANY 与最新公司画像一致（Type-1 规则）
    WITH latest_profile AS (
      SELECT *
      FROM {DB}.{SCHEMA}.COPY_COMPANY_PROFILE_TEAM2
      QUALIFY ROW_NUMBER() OVER (PARTITION BY SYMBOL ORDER BY ID DESC) = 1
    )
    INSERT INTO {DB}.{SCHEMA}.DQ_RESULTS (check_name, status, details, failed_count)
    SELECT 'DIM_COMPANY_T1_MATCH_LATEST',
           IFF(COUNT(*)=0,'PASSED','FAILED'),
           'DIM_COMPANY equals latest profile by symbol (name/industry/sector)',
           COUNT(*)
    FROM {DB}.{SCHEMA}.DIM_COMPANY_TEAM2 dc
    JOIN {DB}.{SCHEMA}.DIM_SYMBOL_TEAM2 ds ON ds.SYMBOL_ID = dc.SYMBOL_ID
    JOIN latest_profile lp ON lp.SYMBOL = ds.SYMBOL
    WHERE NVL(dc.COMPANY_NAME,'') <> NVL(lp.COMPANYNAME,'')
       OR NVL(dc.INDUSTRY,'')     <> NVL(lp.INDUSTRY,'')
       OR NVL(dc.SECTOR,'')       <> NVL(lp.SECTOR,'');

    -- 汇总失败数，有失败则让任务失败
    SET failed_cnt = (
      SELECT COUNT(*)
      FROM {DB}.{SCHEMA}.DQ_RESULTS
      WHERE check_time >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
        AND status = 'FAILED'
    );
    BEGIN
      IF (:failed_cnt > 0) THEN
        SELECT SYSTEM$RAISE_ERROR('DQ_FAILED: ' || :failed_cnt || ' checks failed');
      END IF;
    END;
    """,
)



# DAG Execution Order
[clone_stock_history, clone_company_profile, clone_symbols] >> use_db_schema \
>> upsert_dim_symbol >> upsert_dim_company >> extend_dim_date >> upsert_fact \
>> ensure_results_table >> validate_data_sql