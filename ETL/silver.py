import duckdb

def load_silver(db_path):
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    
    # 1. BASE CLEANING
    con.execute("DROP TABLE IF EXISTS silver.bhavcopy_clean;")
    con.execute("""
        CREATE TABLE silver.bhavcopy_clean AS
        SELECT 
            TRIM(SYMBOL) as symbol, 
            CAST(NULLIF(TRIM(OPEN_PRICE), '') AS DECIMAL(12,2)) as open_price,
            CAST(NULLIF(TRIM(HIGH_PRICE), '') AS DECIMAL(12,2)) as high_price,
            CAST(NULLIF(TRIM(LOW_PRICE), '') AS DECIMAL(12,2)) as low_price,
            CAST(NULLIF(TRIM(CLOSE_PRICE), '') AS DECIMAL(12,2)) as close_price,
            CAST(NULLIF(TRIM(TTL_TRD_QNTY), '') AS BIGINT) as TTL_TRD_QNTY,
            CAST(NULLIF(TRIM(DELIV_QTY), '') AS BIGINT) as DELIV_QTY,
            _file_date
        FROM bronze.bhavcopy_raw
        WHERE TRIM(SERIES) = 'EQ' AND _file_date IS NOT NULL;
    """)

    # 3. WEEKLY DELIVERY CHECK (> 50%)
    con.execute("DROP TABLE IF EXISTS silver.delivery_weekly_check;")
    con.execute("""
        CREATE TABLE silver.delivery_weekly_check AS
        SELECT
            symbol,
            date_trunc('week', _file_date) as week_start,
            (SUM(DELIV_QTY) * 100.0 / NULLIF(SUM(TTL_TRD_QNTY), 0)) as weekly_deliv_per
        FROM silver.bhavcopy_clean
        GROUP BY 1, 2
        HAVING weekly_deliv_per > 50.0;
    """)

    # 2. 6-MONTH BREAKOUT CHECK
    con.execute("DROP TABLE IF EXISTS silver.price_breakout_check;")
    con.execute("""
        CREATE OR REPLACE TABLE silver.price_breakout_check AS
        WITH weekly_closes AS (
            -- 1. Reduce data to only the last trading day of every week
            SELECT b.symbol, b._file_date, b.close_price
            FROM silver.bhavcopy_clean b
            INNER JOIN (
                SELECT symbol, MAX(_file_date) as friday_date
                FROM silver.bhavcopy_clean
                WHERE date_trunc('week', _file_date::TIMESTAMP) <= date_trunc('week', CURRENT_DATE::TIMESTAMP)
                GROUP BY symbol, date_trunc('week', _file_date::TIMESTAMP)
            ) f ON b.symbol = f.symbol AND b._file_date = f.friday_date
        ),
        anchor_friday AS (
            -- 2. Define the anchor: The most recent Friday present in the data (e.g., 2026-04-17)
            SELECT MAX(_file_date) as ref_date FROM weekly_closes
        ),
        price_stats AS (
            -- 3. Calculate high using only the weekly data, relative to our anchor
            SELECT symbol, _file_date, close_price,
                MAX(close_price) OVER (
                    PARTITION BY symbol 
                    ORDER BY _file_date 
                    ROWS BETWEEN 26 PRECEDING AND 1 PRECEDING
                ) as prev_180d_friday_high
            FROM weekly_closes
            WHERE _file_date <= (SELECT ref_date FROM anchor_friday)
        )
        -- 4. Final selection: Filter strictly for the anchor Friday
        SELECT 
            p.symbol, 
            p._file_date, 
            p.close_price, 
            p.prev_180d_friday_high,
            CASE 
                WHEN p.close_price > p.prev_180d_friday_high THEN '✅ YES' 
                ELSE '❌ NO' 
            END as IS_BREAKOUT
        FROM price_stats p
        WHERE p._file_date = (SELECT ref_date FROM anchor_friday);
    """)
    print("✅ Silver layer logic: price_breakout_check complete.")

    # 3. 3X VOLUME SURGE CHECK
    con.execute("DROP TABLE IF EXISTS silver.volume_surge_check;")
    con.execute("""
        CREATE OR REPLACE TABLE silver.volume_surge_check AS
        WITH weekly_aggregates AS (
            -- 1. Sum up total volume for every week per symbol
            SELECT 
                symbol, 
                date_trunc('week', _file_date) as week_start,
                MAX(_file_date) as week_end_date, -- This identifies the Friday/Last day
                SUM(TTL_TRD_QNTY) as weekly_sum
            FROM silver.bhavcopy_clean
            -- Exclude current incomplete week
            WHERE date_trunc('week', _file_date::TIMESTAMP) <= date_trunc('week', CURRENT_DATE::TIMESTAMP)
            GROUP BY 1, 2
        ),
        vol_stats AS (
            -- 2. Calculate average of the weekly sums over the last 26 weeks (~180 days)
            SELECT *,
                AVG(weekly_sum) OVER (
                    PARTITION BY symbol 
                    ORDER BY week_start 
                    ROWS BETWEEN 26 PRECEDING AND 1 PRECEDING
                ) as avg_weekly_vol_180d
            FROM weekly_aggregates
        )
        -- 3. Compare the latest full week's total against the average
        SELECT 
            symbol, 
            week_end_date as _file_date, 
            weekly_sum as TTL_TRD_QNTY, 
            ROUND(avg_weekly_vol_180d, 0) as avg_vol_180d,
            CASE 
                WHEN weekly_sum > (3 * avg_weekly_vol_180d) THEN '✅ YES' 
                ELSE '❌ NO' 
            END as IS_3X_SURGE
        FROM vol_stats
        WHERE week_end_date = (SELECT MAX(week_end_date) FROM weekly_aggregates);
    """)
    print("✅ Silver layer logic: volume_surge_check complete.")

    # 4. WEEKLY DELIVERY CHECK (> 50%)
    con.execute("DROP TABLE IF EXISTS silver.delivery_weekly_check;")
    con.execute("""
        CREATE TABLE silver.delivery_weekly_check AS
        SELECT
            symbol,
            date_trunc('week', _file_date) as week_start,
            (SUM(DELIV_QTY) * 100.0 / NULLIF(SUM(TTL_TRD_QNTY), 0)) as weekly_deliv_per
        FROM silver.bhavcopy_clean
        GROUP BY 1, 2
        HAVING weekly_deliv_per > 50.0;
    """)
    print("✅ Silver layer logic: delivery_weekly_check complete.")

    con.close()