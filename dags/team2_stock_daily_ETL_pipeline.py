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
    dag_id="team2_stock_daily_ETL_pipeline",
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



# 6): Data Validation Task —— Check the number of rows in the dimension table and fact table, and the date range
def validate_integrity():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    def q(sql):
        cur.execute(sql)
        return cur.fetchone()

    print("\n==============================")
    print("Validation Report:")
    print("==============================\n")

    # 1️⃣ DIM_SYMBOL
    copy_total, copy_distinct, dim_rows = q(f"""
        SELECT 
          (SELECT COUNT(*) FROM {DB}.{SCHEMA}.COPY_SYMBOLS_TEAM2),
          (SELECT COUNT(DISTINCT SYMBOL) FROM {DB}.{SCHEMA}.COPY_SYMBOLS_TEAM2),
          (SELECT COUNT(*) FROM {DB}.{SCHEMA}.DIM_SYMBOL_TEAM2)
    """)
    result_symbol = "✅Consistent" if copy_distinct == dim_rows else "⚠️Inconsistent"
    print(f"[DIM_SYMBOL]\nCOPY SYMBOL Total Rows: {copy_total}, Distinct Rows: {copy_distinct}, DIM SYMBOL Rows: {dim_rows} → {result_symbol}\n")

    # 2️⃣ DIM_COMPANY
    copy_total, copy_distinct, dim_rows = q(f"""
        SELECT 
          (SELECT COUNT(*) FROM {DB}.{SCHEMA}.COPY_COMPANY_PROFILE_TEAM2),
          (SELECT COUNT(DISTINCT SYMBOL) FROM {DB}.{SCHEMA}.COPY_COMPANY_PROFILE_TEAM2),
          (SELECT COUNT(*) FROM {DB}.{SCHEMA}.DIM_COMPANY_TEAM2)
    """)
    result_company = "✅Consistent" if copy_distinct == dim_rows else "⚠️Inconsistent"
    print(f"[DIM_COMPANY]\nCOPY COMPANY Total Rows: {copy_total}, Distinct Rows: {copy_distinct}, DIM COMPANY Rows: {dim_rows} → {result_company}\n")

    # 3️⃣ DIM_DATE
    copy_dates, dim_dates, copy_max, dim_max = q(f"""
        SELECT 
          (SELECT COUNT(DISTINCT DATE) FROM {DB}.{SCHEMA}.COPY_STOCK_HISTORY_TEAM2),
          (SELECT COUNT(*) FROM {DB}.{SCHEMA}.DIM_DATE_TEAM2),
          (SELECT MAX(DATE) FROM {DB}.{SCHEMA}.COPY_STOCK_HISTORY_TEAM2),
          (SELECT MAX(FULL_DATE) FROM {DB}.{SCHEMA}.DIM_DATE_TEAM2)
    """)
    result_date = "✅Complete Coverage" if dim_dates >= copy_dates else "⚠️Missing Dates"
    print(f"[DIM_DATE]\nCOPY Unique Dates: {copy_dates}, DIM Date Rows: {dim_dates}\nMaximum Date: COPY={copy_max}, DIM={dim_max} → {result_date}\n")

    # 4️⃣ FACT_STOCK_DAILY
    copy_total, copy_distinct, fact_rows = q(f"""
        SELECT
          (SELECT COUNT(*) FROM {DB}.{SCHEMA}.COPY_STOCK_HISTORY_TEAM2),
          (SELECT COUNT(DISTINCT SYMBOL || TO_VARCHAR(DATE)) FROM {DB}.{SCHEMA}.COPY_STOCK_HISTORY_TEAM2),
          (SELECT COUNT(*) FROM {DB}.{SCHEMA}.FACT_STOCK_DAILY_TEAM2)
    """)
    duplicate_rows = copy_total - copy_distinct
    result_fact = "✅Consistent" if fact_rows == copy_distinct else "⚠️Inconsistent"
    print(f"[FACT_STOCK_DAILY]\nCOPY Total Rows: {copy_total}, Distinct Rows: {copy_distinct}, FACT Rows: {fact_rows}, Duplicate Rows: {duplicate_rows} → {result_fact}\n")

    print("\n========== DATA INTEGRITY REPORT ==========\n")

    # 1️⃣ Foreign Key Integrity
    missing_symbol_fk = q(f"""
      SELECT COUNT(*) FROM {DB}.{SCHEMA}.FACT_STOCK_DAILY_TEAM2 f
      LEFT JOIN {DB}.{SCHEMA}.DIM_SYMBOL_TEAM2 s ON f.SYMBOL_ID = s.SYMBOL_ID
      WHERE s.SYMBOL_ID IS NULL
    """)[0]
    print(f"[FK FACT→DIM_SYMBOL] {'✅' if missing_symbol_fk==0 else '⚠️'} Missing: {missing_symbol_fk}")

    missing_date_fk = q(f"""
      SELECT COUNT(*) FROM {DB}.{SCHEMA}.FACT_STOCK_DAILY_TEAM2 f
      LEFT JOIN {DB}.{SCHEMA}.DIM_DATE_TEAM2 d ON f.DATE_ID = d.DATE_ID
      WHERE d.DATE_ID IS NULL
    """)[0]
    print(f"[FK FACT→DIM_DATE]   {'✅' if missing_date_fk==0 else '⚠️'} Missing: {missing_date_fk}")

    # 2️⃣ Uniqueness
    dup_keys = q(f"""
      SELECT COUNT(*) - COUNT(DISTINCT SYMBOL_ID || '-' || DATE_ID)
      FROM {DB}.{SCHEMA}.FACT_STOCK_DAILY_TEAM2
    """)[0]
    print(f"[UNIQUENESS FACT PK] {'✅' if dup_keys==0 else '⚠️'} Duplicate Primary Keys: {dup_keys}")

    # 3️⃣ Null Checks
    nulls = q(f"""
      SELECT
        COUNT_IF(SYMBOL_ID IS NULL),
        COUNT_IF(DATE_ID IS NULL),
        COUNT_IF(CLOSE IS NULL)
      FROM {DB}.{SCHEMA}.FACT_STOCK_DAILY_TEAM2
    """)
    print(f"[NOT NULL Checks]    SYMBOL_ID={nulls[0]}, DATE_ID={nulls[1]}, CLOSE={nulls[2]} → {'✅' if sum(nulls)==0 else '⚠️'}")

    # 4️⃣ Value Range Sanity
    sanity = q(f"""
      SELECT
        COUNT_IF(OPEN < 0 OR HIGH < 0 OR LOW < 0 OR CLOSE < 0),
        COUNT_IF(VOLUME < 0),
        COUNT_IF(HIGH < LOW)
      FROM {DB}.{SCHEMA}.FACT_STOCK_DAILY_TEAM2
    """)
    ok = (sanity[0]==0 and sanity[1]==0 and sanity[2]==0)
    print(f"[VALUE Sanity]       neg_price={sanity[0]}, neg_vol={sanity[1]}, high<low={sanity[2]} → {'✅' if ok else '⚠️'}")

    # 5️⃣ Freshness
    freshness = q(f"""
      SELECT
        (SELECT MAX(DATE) FROM {DB}.{SCHEMA}.COPY_STOCK_HISTORY_TEAM2),
        (SELECT MAX(FULL_DATE) FROM {DB}.{SCHEMA}.DIM_DATE_TEAM2),
        (SELECT MAX(dd.FULL_DATE)
           FROM {DB}.{SCHEMA}.FACT_STOCK_DAILY_TEAM2 f
           JOIN {DB}.{SCHEMA}.DIM_DATE_TEAM2 dd ON dd.DATE_ID = f.DATE_ID)
    """)
    print(f"[FRESHNESS]          src_max={freshness[0]}, dim_max={freshness[1]}, fact_max={freshness[2]} → {'✅' if freshness[2] is not None else '⚠️'}")

    # 6️⃣ Date Coverage
    miss_dates = q(f"""
      SELECT COUNT(*) FROM (
        SELECT DISTINCT DATE FROM {DB}.{SCHEMA}.COPY_STOCK_HISTORY_TEAM2
      ) s LEFT JOIN {DB}.{SCHEMA}.DIM_DATE_TEAM2 d ON d.FULL_DATE = s.DATE
      WHERE d.FULL_DATE IS NULL
    """)[0]
    print(f"[DATE Coverage]      missing_in_dim={miss_dates} → {'✅' if miss_dates==0 else '⚠️'}")

    # 7️⃣ DIM_COMPANY Type-1 Consistency
    company_mismatch = q(f"""
      WITH latest_profile AS (
        SELECT t.* FROM (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY SYMBOL ORDER BY ID DESC) rn
          FROM {DB}.{SCHEMA}.COPY_COMPANY_PROFILE_TEAM2
        ) t WHERE rn=1
      )
      SELECT COUNT(*)
      FROM {DB}.{SCHEMA}.DIM_COMPANY_TEAM2 dc
      JOIN {DB}.{SCHEMA}.DIM_SYMBOL_TEAM2 ds ON ds.SYMBOL_ID = dc.SYMBOL_ID
      JOIN latest_profile lp ON lp.SYMBOL = ds.SYMBOL
      WHERE NVL(dc.COMPANY_NAME,'') <> NVL(lp.COMPANYNAME,'')
         OR NVL(dc.INDUSTRY,'')     <> NVL(lp.INDUSTRY,'')
         OR NVL(dc.SECTOR,'')       <> NVL(lp.SECTOR,'')
    """)[0]
    print(f"[DIM_COMPANY T1]     {'✅' if company_mismatch==0 else '⚠️'} Inconsistent: {company_mismatch}")

    print("\n==============================")
    print("Validation Completed")
    print("==============================\n")

    cur.close()
    conn.close()



# Airflow Task Node
validate_data = PythonOperator(
    task_id="validate_integrity",
    python_callable=validate_integrity,
)


# DAG Execution Order
[clone_stock_history,clone_company_profile,clone_symbols] >> use_db_schema >> upsert_dim_symbol >> upsert_dim_company >> extend_dim_date >> upsert_fact >> validate_data