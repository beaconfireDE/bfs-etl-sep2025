MERGE INTO fact_market_daily AS tgt
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
);