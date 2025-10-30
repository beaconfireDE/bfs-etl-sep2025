CREATE OR REPLACE TABLE dim_date (
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
);