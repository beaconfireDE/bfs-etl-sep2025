-- Data Integrity Validation Checks
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
WHERE f.DATE_ID IS NULL;
