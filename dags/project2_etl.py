from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime
import re

# -----------------------------------------------------------------------------
# DAG CONFIGURATION
# -----------------------------------------------------------------------------
default_args = {
    "owner": "team3",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 30),
    "retries": 1,
}

SNOWFLAKE_CONN_ID = "snowflake_conn"
SNOWFLAKE_ROLE = "DE_DEVELOPER_0928"
SNOWFLAKE_WAREHOUSE = "DE_0928_WH"
SNOWFLAKE_DATABASE = "AIRFLOW0928"
SNOWFLAKE_SCHEMA = "DEV"

TEAM_SUFFIX = "_team3"

# -----------------------------------------------------------------------------
# SQL Statements - Embedded in Python file
# -----------------------------------------------------------------------------
SQL_STATEMENTS = {
    "create_dim_company.sql": """CREATE TABLE IF NOT EXISTS dim_company (
    COMPANY_ID            NUMBER(38,0) AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    ID                     NUMBER(38,0),
    SYMBOL                 VARCHAR(20),
    COMPANYNAME            VARCHAR(200),
    SECTOR                 VARCHAR(100),
    INDUSTRY               VARCHAR(100),
    CEO                    VARCHAR(100),
    WEBSITE                VARCHAR(200),
    EXCHANGE               VARCHAR(50),
    DESCRIPTION            VARCHAR(2000),
    PRICE                  FLOAT,
    BETA                   FLOAT,
    VOLAVG                 NUMBER(38,0),
    MKTCAP                 NUMBER(38,0),
    LASTDIV                FLOAT,
    RANGE                  VARCHAR(50),
    CHANGES                FLOAT,
    DCFDIFF                FLOAT,
    DCF                    FLOAT,
    LOAD_TS                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);""",

    "create_dim_date.sql": """CREATE TABLE IF NOT EXISTS dim_date (
    DATE_ID               NUMBER(38,0) AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    FULL_DATE              DATE,
    YEAR                   NUMBER(4,0),
    QUARTER                NUMBER(1,0),
    MONTH                  NUMBER(2,0),
    DAY                    NUMBER(2,0),
    DAY_OF_WEEK            NUMBER(1,0),
    WEEK_OF_YEAR           NUMBER(2,0),
    MONTH_NAME             VARCHAR(15),
    DAY_NAME               VARCHAR(15),
    LOAD_TS                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);""",

    "create_dim_symbol.sql": """CREATE TABLE IF NOT EXISTS dim_symbol (
    SYMBOL_ID             NUMBER(38,0) AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    SYMBOL                 VARCHAR(20),
    NAME                   VARCHAR(200),
    EXCHANGE               VARCHAR(50),
    LOAD_TS                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);""",

    "create_fact_market_daily.sql": """CREATE TABLE IF NOT EXISTS fact_market_daily (
    FACT_ID               NUMBER(38,0) AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    SYMBOL_ID             NUMBER(38,0),
    COMPANY_ID            NUMBER(38,0),
    DATE_ID               NUMBER(38,0),
    SYMBOL                 VARCHAR(20),
    TRADE_DATE             DATE,
    OPEN                   FLOAT,
    HIGH                   FLOAT,
    LOW                    FLOAT,
    CLOSE                  FLOAT,
    ADJCLOSE               FLOAT,
    VOLUME                 NUMBER(38,0),
    LOAD_TS                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (SYMBOL_ID)  REFERENCES AIRFLOW0928.DEV.dim_symbol_team3(SYMBOL_ID),
    FOREIGN KEY (COMPANY_ID) REFERENCES AIRFLOW0928.DEV.dim_company_team3(COMPANY_ID),
    FOREIGN KEY (DATE_ID)    REFERENCES AIRFLOW0928.DEV.dim_date_team3(DATE_ID)
);""",

    "update_dim_company.sql": """MERGE INTO dim_company AS tgt
USING (
    SELECT 
        ID, SYMBOL, COMPANYNAME, SECTOR, INDUSTRY, CEO, WEBSITE, EXCHANGE, DESCRIPTION,
        PRICE, BETA, VOLAVG, MKTCAP, LASTDIV, RANGE, CHANGES, DCFDIFF, DCF
    FROM US_STOCK_DAILY.DCCM.Company_Profile
) AS src
ON tgt.SYMBOL = src.SYMBOL

WHEN MATCHED THEN UPDATE SET
    tgt.COMPANYNAME = src.COMPANYNAME,
    tgt.SECTOR      = src.SECTOR,
    tgt.INDUSTRY    = src.INDUSTRY,
    tgt.CEO         = src.CEO,
    tgt.WEBSITE     = src.WEBSITE,
    tgt.EXCHANGE    = src.EXCHANGE,
    tgt.DESCRIPTION = src.DESCRIPTION,
    tgt.PRICE       = src.PRICE,
    tgt.BETA        = src.BETA,
    tgt.VOLAVG      = src.VOLAVG,
    tgt.MKTCAP      = src.MKTCAP,
    tgt.LASTDIV     = src.LASTDIV,
    tgt.RANGE       = src.RANGE,
    tgt.CHANGES     = src.CHANGES,
    tgt.DCFDIFF     = src.DCFDIFF,
    tgt.DCF         = src.DCF,
    tgt.LOAD_TS     = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    ID, SYMBOL, COMPANYNAME, SECTOR, INDUSTRY, CEO, WEBSITE, EXCHANGE, DESCRIPTION,
    PRICE, BETA, VOLAVG, MKTCAP, LASTDIV, RANGE, CHANGES, DCFDIFF, DCF, LOAD_TS
)
VALUES (
    src.ID, src.SYMBOL, src.COMPANYNAME, src.SECTOR, src.INDUSTRY, src.CEO, src.WEBSITE,
    src.EXCHANGE, src.DESCRIPTION, src.PRICE, src.BETA, src.VOLAVG, src.MKTCAP, src.LASTDIV,
    src.RANGE, src.CHANGES, src.DCFDIFF, src.DCF, CURRENT_TIMESTAMP()
);""",

    "update_dim_date.sql": """MERGE INTO dim_date AS tgt
USING (
    SELECT DISTINCT
        TRY_TO_DATE(DATE) AS FULL_DATE,
        YEAR(TRY_TO_DATE(DATE)) AS YEAR,
        QUARTER(TRY_TO_DATE(DATE)) AS QUARTER,
        MONTH(TRY_TO_DATE(DATE)) AS MONTH,
        DAY(TRY_TO_DATE(DATE)) AS DAY,
        DAYOFWEEK(TRY_TO_DATE(DATE)) AS DAY_OF_WEEK,
        WEEKOFYEAR(TRY_TO_DATE(DATE)) AS WEEK_OF_YEAR,
        TO_VARCHAR(TO_DATE(DATE), 'MMMM') AS MONTH_NAME,
        TO_VARCHAR(TO_DATE(DATE), 'DY') AS DAY_NAME
    FROM US_STOCK_DAILY.DCCM.Stock_History
    WHERE TRY_TO_DATE(DATE) IS NOT NULL
) AS src
ON tgt.FULL_DATE = src.FULL_DATE

WHEN MATCHED THEN UPDATE SET
    tgt.LOAD_TS = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    FULL_DATE, YEAR, QUARTER, MONTH, DAY, DAY_OF_WEEK, WEEK_OF_YEAR,
    MONTH_NAME, DAY_NAME, LOAD_TS
)
VALUES (
    src.FULL_DATE, src.YEAR, src.QUARTER, src.MONTH, src.DAY,
    src.DAY_OF_WEEK, src.WEEK_OF_YEAR, src.MONTH_NAME, src.DAY_NAME, CURRENT_TIMESTAMP()
);""",

    "update_dim_symbol.sql": """MERGE INTO dim_symbol AS tgt
USING (
    SELECT SYMBOL, NAME, EXCHANGE
    FROM US_STOCK_DAILY.DCCM.Symbols
) AS src
ON tgt.SYMBOL = src.SYMBOL

WHEN MATCHED THEN UPDATE SET
    tgt.NAME     = src.NAME,
    tgt.EXCHANGE = src.EXCHANGE,
    tgt.LOAD_TS  = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (SYMBOL, NAME, EXCHANGE, LOAD_TS)
VALUES (src.SYMBOL, src.NAME, src.EXCHANGE, CURRENT_TIMESTAMP());

MERGE INTO AIRFLOW0928.DEV.dim_date_team3 AS tgt
USING (
    SELECT DISTINCT
        TRY_TO_DATE(DATE) AS FULL_DATE,
        YEAR(TRY_TO_DATE(DATE)) AS YEAR,
        QUARTER(TRY_TO_DATE(DATE)) AS QUARTER,
        MONTH(TRY_TO_DATE(DATE)) AS MONTH,
        DAY(TRY_TO_DATE(DATE)) AS DAY,
        DAYOFWEEK(TRY_TO_DATE(DATE)) AS DAY_OF_WEEK,
        WEEKOFYEAR(TRY_TO_DATE(DATE)) AS WEEK_OF_YEAR,
        TO_VARCHAR(TO_DATE(DATE), 'MMMM') AS MONTH_NAME,
        TO_VARCHAR(TO_DATE(DATE), 'DY') AS DAY_NAME
    FROM US_STOCK_DAILY.DCCM.Stock_History
    WHERE TRY_TO_DATE(DATE) IS NOT NULL
) AS src
ON tgt.FULL_DATE = src.FULL_DATE

WHEN MATCHED THEN UPDATE SET
    tgt.LOAD_TS = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    FULL_DATE, YEAR, QUARTER, MONTH, DAY, DAY_OF_WEEK, WEEK_OF_YEAR,
    MONTH_NAME, DAY_NAME, LOAD_TS
)
VALUES (
    src.FULL_DATE, src.YEAR, src.QUARTER, src.MONTH, src.DAY,
    src.DAY_OF_WEEK, src.WEEK_OF_YEAR, src.MONTH_NAME, src.DAY_NAME, CURRENT_TIMESTAMP()
);""",

    "update_fact_market_daily .sql": """MERGE INTO fact_market_daily AS tgt
USING (
    SELECT 
        sym.SYMBOL_ID,
        comp.COMPANY_ID,
        dt.DATE_ID,
        sh.SYMBOL,
        TRY_TO_DATE(sh.DATE) AS TRADE_DATE,
        MAX(sh.OPEN) AS OPEN,
        MAX(sh.HIGH) AS HIGH,
        MAX(sh.LOW) AS LOW,
        MAX(sh.CLOSE) AS CLOSE,
        MAX(sh.ADJCLOSE) AS ADJCLOSE,
        MAX(sh.VOLUME) AS VOLUME
    FROM US_STOCK_DAILY.DCCM.Stock_History sh
    LEFT JOIN AIRFLOW0928.DEV.dim_symbol_team3 sym ON sh.SYMBOL = sym.SYMBOL
    LEFT JOIN AIRFLOW0928.DEV.dim_company_team3 comp ON sh.SYMBOL = comp.SYMBOL
    LEFT JOIN AIRFLOW0928.DEV.dim_date_team3 dt ON TRY_TO_DATE(sh.DATE) = dt.FULL_DATE
    WHERE TRY_TO_DATE(sh.DATE) IS NOT NULL
    GROUP BY sym.SYMBOL_ID, comp.COMPANY_ID, dt.DATE_ID, sh.SYMBOL, TRY_TO_DATE(sh.DATE)
) AS src
ON tgt.SYMBOL = src.SYMBOL AND tgt.TRADE_DATE = src.TRADE_DATE

WHEN MATCHED AND (
    NVL(tgt.OPEN,0)      != NVL(src.OPEN,0) OR
    NVL(tgt.HIGH,0)      != NVL(src.HIGH,0) OR
    NVL(tgt.LOW,0)       != NVL(src.LOW,0) OR
    NVL(tgt.CLOSE,0)     != NVL(src.CLOSE,0) OR
    NVL(tgt.ADJCLOSE,0)  != NVL(src.ADJCLOSE,0) OR
    NVL(tgt.VOLUME,0)    != NVL(src.VOLUME,0)
) THEN UPDATE SET
    tgt.OPEN      = src.OPEN,
    tgt.HIGH      = src.HIGH,
    tgt.LOW       = src.LOW,
    tgt.CLOSE     = src.CLOSE,
    tgt.ADJCLOSE  = src.ADJCLOSE,
    tgt.VOLUME    = src.VOLUME,
    tgt.SYMBOL_ID = src.SYMBOL_ID,
    tgt.COMPANY_ID= src.COMPANY_ID,
    tgt.DATE_ID   = src.DATE_ID,
    tgt.LOAD_TS   = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    SYMBOL_ID, COMPANY_ID, DATE_ID, SYMBOL, TRADE_DATE,
    OPEN, HIGH, LOW, CLOSE, ADJCLOSE, VOLUME, LOAD_TS
)
VALUES (
    src.SYMBOL_ID, src.COMPANY_ID, src.DATE_ID, src.SYMBOL,
    src.TRADE_DATE, src.OPEN, src.HIGH, src.LOW, src.CLOSE,
    src.ADJCLOSE, src.VOLUME, CURRENT_TIMESTAMP()
);""",

    "val_post_update.sql": """-- 1 Row count validation -------------------------------------------------
SELECT 
    (SELECT COUNT(*) FROM fact_market_daily) AS target_count,
    (SELECT COUNT(*) 
     FROM US_STOCK_DAILY.DCCM.Stock_History sh
     WHERE TRY_TO_DATE(sh.DATE) IS NOT NULL) AS source_count,
    CASE 
        WHEN ABS(target_count - source_count) <= 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS rowcount_check_result;

-- 2 Null check on key columns --------------------------------------------
SELECT 
    'NULL_CHECK' AS check_type,
    COUNT_IF(symbol IS NULL OR trade_date IS NULL) AS null_key_records,
    CASE 
        WHEN COUNT_IF(symbol IS NULL OR trade_date IS NULL) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS null_check_result
FROM fact_market_daily;

-- 3 Referential integrity ------------------------------------------------
SELECT 
    'FK_SYMBOL' AS check_type,
    COUNT(*) AS orphan_symbol_records,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS fk_symbol_check_result
FROM fact_market_daily f
LEFT JOIN dim_symbol s 
    ON f.symbol = s.symbol
WHERE s.symbol IS NULL;

SELECT 
    'FK_COMPANY' AS check_type,
    COUNT(*) AS orphan_company_records,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS fk_company_check_result
FROM fact_market_daily f
LEFT JOIN dim_company c 
    ON f.company_id = c.company_id
WHERE c.company_id IS NULL;

-- 4 Compare sampling records ------------------------------------------------
WITH sampled_source AS (
    SELECT *
    FROM US_STOCK_DAILY.DCCM.Stock_History
    SAMPLE (2)  -- ← 2% random sample, adjust as needed
),
compared AS (
    SELECT 
        h.SYMBOL,
        f.symbol_ID,
        f.date_ID
    FROM sampled_source h
    JOIN dim_symbol s
      ON UPPER(h.SYMBOL) = UPPER(s.symbol)
    JOIN fact_market_daily f
      ON s.symbol_ID = f.symbol_ID
     AND TO_NUMBER(TO_CHAR(h.DATE, 'YYYYMMDD')) = f.date_ID
    WHERE
        ABS(h.OPEN      - f.open)      > 1e-8 OR
        ABS(h.HIGH      - f.high)      > 1e-8 OR
        ABS(h.LOW       - f.low)       > 1e-8 OR
        ABS(h.CLOSE     - f.close)     > 1e-8 OR
        ABS(h.ADJCLOSE  - f.adjclose)  > 1e-8 OR
        ABS(h.VOLUME    - f.volume)    > 1
),
total_sample AS (
    SELECT COUNT(*) AS total_rows FROM sampled_source h
    JOIN dim_symbol s ON UPPER(h.SYMBOL) = UPPER(s.symbol)
    JOIN fact_market_daily f
      ON s.symbol_ID = f.symbol_ID
     AND TO_NUMBER(TO_CHAR(h.DATE, 'YYYYMMDD')) = f.date_ID
)
SELECT
    (SELECT COUNT(*) FROM compared)  AS mismatched_rows,
    (SELECT total_rows FROM total_sample) AS total_sampled_rows,
    ROUND(
        (SELECT COUNT(*) FROM compared)::FLOAT 
        / NULLIF((SELECT total_rows FROM total_sample), 0),
        6
    ) AS mismatch_ratio,
    CASE 
        WHEN (SELECT COUNT(*) FROM compared)::FLOAT 
             / NULLIF((SELECT total_rows FROM total_sample), 1) <= 0
            THEN 'PASS'
        ELSE 'FAIL'
    END AS consistency_check_result;""",

    "validate_data_integrity.sql": """-- Data Integrity Validation Checks
-- This query returns issues count for each validation rule
-- If all checks pass, all rows should have issues = 0

-- 1. Data Range Check - prices should be positive or zero
SELECT 'Data Range - Negative Prices' AS check_type, COUNT(*) AS issues
FROM fact_market_daily
WHERE OPEN < 0 OR HIGH < 0 OR LOW < 0 OR CLOSE < 0 OR ADJCLOSE < 0

UNION ALL

-- 2. Data Range Check - volume should be non-negative
SELECT 'Data Range - Negative Volume' AS check_type, COUNT(*) AS issues
FROM fact_market_daily
WHERE VOLUME < 0

UNION ALL

-- 3. Data Range Check - high should be >= low
SELECT 'Data Range - High < Low' AS check_type, COUNT(*) AS issues
FROM fact_market_daily
WHERE HIGH < LOW

UNION ALL

-- 4. Data Range Check - high should be >= open and high should be >= close
SELECT 'Data Range - High < Open/Close' AS check_type, COUNT(*) AS issues
FROM fact_market_daily
WHERE HIGH < OPEN OR HIGH < CLOSE

UNION ALL

-- 5. Data Range Check - low should be <= open and low should be <= close
SELECT 'Data Range - Low > Open/Close' AS check_type, COUNT(*) AS issues
FROM fact_market_daily
WHERE LOW > OPEN OR LOW > CLOSE

UNION ALL

-- 6. Duplicate Check - fact table (SYMBOL + TRADE_DATE should be unique)
SELECT 'Duplicate - fact_market_daily' AS check_type, 
       COUNT(*) - COUNT(DISTINCT SYMBOL, TRADE_DATE) AS issues
FROM fact_market_daily

UNION ALL

-- 7. Duplicate Check - dim_symbol (SYMBOL should be unique)
SELECT 'Duplicate - dim_symbol' AS check_type, 
       COUNT(*) - COUNT(DISTINCT SYMBOL) AS issues
FROM dim_symbol

UNION ALL

-- 8. Duplicate Check - dim_company (SYMBOL should be unique)
SELECT 'Duplicate - dim_company' AS check_type, 
       COUNT(*) - COUNT(DISTINCT SYMBOL) AS issues
FROM dim_company

UNION ALL

-- 9. Referential Integrity Check - fact table should have valid foreign keys
SELECT 'Referential Integrity - Missing SYMBOL_ID' AS check_type, COUNT(*) AS issues
FROM fact_market_daily f
WHERE f.SYMBOL_ID IS NULL

UNION ALL

SELECT 'Referential Integrity - Missing COMPANY_ID' AS check_type, COUNT(*) AS issues
FROM fact_market_daily f
WHERE f.COMPANY_ID IS NULL

UNION ALL

SELECT 'Referential Integrity - Missing DATE_ID' AS check_type, COUNT(*) AS issues
FROM fact_market_daily f
WHERE f.DATE_ID IS NULL;"""
}

# -----------------------------------------------------------------------------
# Helper: Load SQL from embedded statements and replace table names
# -----------------------------------------------------------------------------
def load_sql(file_name: str) -> str:
    """Loads SQL from embedded statements and appends _team3 suffix to target tables."""
    if file_name not in SQL_STATEMENTS:
        raise ValueError(f"SQL file '{file_name}' not found in embedded statements")
    
    sql = SQL_STATEMENTS[file_name]

    for tbl in ["dim_company", "dim_date", "dim_symbol", "fact_market_daily"]:
        pattern = rf"\b{tbl}\b"
        sql = re.sub(pattern, f"{tbl}{TEAM_SUFFIX}", sql)

    return sql
# -------------------------------------------------------------------------
# Generic Validation Checker
# -------------------------------------------------------------------------
def run_validation(file_name: str):
    """Execute validation SQL and fail DAG if any FAIL or issues > 0."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    sql = load_sql(file_name)
    cursor.execute(sql)
    results = cursor.fetchall()
    col_names = [desc[0] for desc in cursor.description]

    fail_rows = []
    for row in results:
        row_dict = dict(zip(col_names, row))
        for val in row_dict.values():
            if (isinstance(val, str) and val.strip().upper() == "FAIL") or (
                isinstance(val, (int, float)) and val > 0
            ):
                fail_rows.append(row_dict)
                break

    cursor.close()
    conn.close()

    if fail_rows:
        raise AirflowException(f"Validation failed in {file_name}:\n{fail_rows}")
    else:
        print(f"{file_name} passed all data validation checks.")

# -----------------------------------------------------------------------------
# DAG DEFINITION
# -----------------------------------------------------------------------------
with DAG(
    dag_id="etl_team3_snowflake_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="ETL automation DAG for Snowflake tables (Team3)",
    tags=["snowflake", "etl", "team3"],
) as dag:

    # ------------------------- CREATE TASKS ----------------------------------
    create_dim_company = SnowflakeOperator(
        task_id="create_dim_company",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        sql=load_sql("create_dim_company.sql"),
    )

    create_dim_date = SnowflakeOperator(
        task_id="create_dim_date",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        sql=load_sql("create_dim_date.sql"),
    )

    create_dim_symbol = SnowflakeOperator(
        task_id="create_dim_symbol",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        sql=load_sql("create_dim_symbol.sql"),
    )

    create_fact_market_daily = SnowflakeOperator(
        task_id="create_fact_market_daily",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        sql=load_sql("create_fact_market_daily.sql"),
    )

    # ------------------------- UPDATE TASKS ----------------------------------
    update_dim_company = SnowflakeOperator(
        task_id="update_dim_company",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        sql=load_sql("update_dim_company.sql"),
    )

    update_dim_date = SnowflakeOperator(
        task_id="update_dim_date",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        sql=load_sql("update_dim_date.sql"),
    )

    update_dim_symbol = SnowflakeOperator(
        task_id="update_dim_symbol",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        sql=load_sql("update_dim_symbol.sql"),
    )

    update_fact_market_daily = SnowflakeOperator(
        task_id="update_fact_market_daily",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        sql=load_sql("update_fact_market_daily.sql"),
    )

    # ------------------------- VALIDATION TASKS ------------------------------
    validate_consistency = PythonOperator(
        task_id="validate_consistency_sampling",
        python_callable=lambda **_: run_validation("val_post_update.sql"),
        provide_context=True,
    )

    validate_data_integrity = PythonOperator(
        task_id="validate_data_integrity_rules",
        python_callable=lambda **_: run_validation("validate_data_integrity.sql"),
        provide_context=True,
    )

    # ------------------------- TASK DEPENDENCIES -----------------------------
    (
        [create_dim_company, create_dim_date, create_dim_symbol]
        >> create_fact_market_daily
        >> [update_dim_company, update_dim_date, update_dim_symbol]
        >> update_fact_market_daily
        >> validate_consistency
        >> validate_data_integrity
    )
