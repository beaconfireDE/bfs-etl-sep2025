-- 1 Row count validation -------------------------------------------------
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
    END AS consistency_check_result;