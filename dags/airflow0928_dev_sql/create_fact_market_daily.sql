CREATE OR REPLACE TABLE fact_market_daily (
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
);