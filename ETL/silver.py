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

    # 2. 6-MONTH BREAKOUT CHECK WITH RSI AS AN EXPLICIT COLUMN
    con.execute("DROP TABLE IF EXISTS silver.price_breakout_check;")
    con.execute("""
        CREATE OR REPLACE TABLE silver.price_breakout_check AS
        WITH weekly_closes AS (
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
            SELECT MAX(_file_date) as ref_date FROM weekly_closes
        ),
        rsi_raw AS (
            SELECT symbol, _file_date, close_price,
                close_price - LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY _file_date) as change
            FROM weekly_closes
        ),
        rsi_components AS (
            SELECT symbol, _file_date, close_price,
                CASE WHEN change > 0 THEN change ELSE 0 END as gain,
                CASE WHEN change < 0 THEN -change ELSE 0 END as loss,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY _file_date) as rn
            FROM rsi_raw
        ),
        rsi_smoothed AS (
            SELECT symbol, _file_date, close_price, rn,
                SUM(gain * POWER(14.0 / 13.0, rn)) 
                    OVER (PARTITION BY symbol ORDER BY _file_date ROWS BETWEEN 150 PRECEDING AND CURRENT ROW)
                    * POWER(13.0 / 14.0, rn) as avg_gain,
                SUM(loss * POWER(14.0 / 13.0, rn)) 
                    OVER (PARTITION BY symbol ORDER BY _file_date ROWS BETWEEN 150 PRECEDING AND CURRENT ROW)
                    * POWER(13.0 / 14.0, rn) as avg_loss
            FROM rsi_components
        ),
        rsi_calc AS (
            SELECT symbol, _file_date, close_price,
                ROUND(CASE 
                    WHEN rn < 14 THEN NULL 
                    WHEN avg_loss = 0 THEN 100
                    ELSE 100 - (100 / (1 + (avg_gain / avg_loss)))
                END, 2) as rsi
            FROM rsi_smoothed
        ),
        price_stats AS (
            SELECT r.symbol, r._file_date, r.close_price, r.rsi,
                MAX(r.close_price) OVER ( 
                    PARTITION BY r.symbol 
                    ORDER BY r._file_date 
                    ROWS BETWEEN 26 PRECEDING AND 1 PRECEDING
                ) as prev_180d_friday_high,
                ROUND(AVG(r.close_price) OVER (
                    PARTITION BY r.symbol
                    ORDER BY r._file_date
                    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                ),2) as "50w_Moving_avg"
            FROM rsi_calc r
        )
        SELECT 
            p.symbol, 
            p._file_date, 
            p.close_price, 
            p.prev_180d_friday_high,
            p."50w_Moving_avg",
            p.rsi,
            CASE 
                WHEN p.close_price > p.prev_180d_friday_high THEN '✅ YES' 
                ELSE '❌ NO' 
            END as IS_BREAKOUT
        FROM price_stats p
        WHERE p._file_date = (SELECT ref_date FROM anchor_friday)
          AND p.rsi > 45.0;
    """)
    print("✅ Silver layer logic: price_breakout_check complete with dedicated RSI column.")
    
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