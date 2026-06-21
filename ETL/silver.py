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
    print("✅ Silver layer: Base Cleaning Completed.")

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
    print("✅ Silver layer: Weekly Delivery Check Completed.")

    # 2. 6-MONTH BREAKOUT CHECK WITH RECURSIVE WILDER'S RSI & 50W EMA
    con.execute("DROP TABLE IF EXISTS silver.price_breakout_check;")
    con.execute("""
        CREATE OR REPLACE TABLE silver.price_breakout_check AS
        WITH RECURSIVE weekly_closes AS (
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
            -- 2. Define the anchor: The most recent Friday present in the data
            SELECT MAX(_file_date) as ref_date FROM weekly_closes
        ),
        rsi_raw AS (
            -- Calculate price change from previous week
            SELECT symbol, _file_date, close_price,
                close_price - LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY _file_date) as change
            FROM weekly_closes
        ),
        rsi_components AS (
            -- Use PARTITION BY inside ROW_NUMBER so every single symbol starts exactly at rn = 1
            SELECT symbol, _file_date, close_price,
                CASE WHEN change > 0 THEN change ELSE 0 END as gain,
                CASE WHEN change < 0 THEN -change ELSE 0 END as loss,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY _file_date) as rn
            FROM rsi_raw
        ),
        -- Intermediate step to compute the accurate Simple Moving Average baselines per stock
        baselines AS (
            SELECT symbol, _file_date, close_price, rn, gain, loss,
                AVG(gain) OVER (PARTITION BY symbol ORDER BY _file_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as sma_gain,
                AVG(loss) OVER (PARTITION BY symbol ORDER BY _file_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as sma_loss,
                AVG(close_price) OVER (PARTITION BY symbol ORDER BY _file_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) as sma_ema
            FROM rsi_components
        ),
        -- Recursive CTE to compute the smoothing loops
        indicators_recursive AS (
            -- Anchor Member: Initialize at each stock's relative 50th historical week
            SELECT 
                symbol, _file_date, close_price, rn, gain, loss,
                sma_gain as avg_gain,
                sma_loss as avg_loss,
                sma_ema as ema_val
            FROM baselines
            WHERE rn = 50
            
            UNION ALL
            
            -- Recursive Member: Smooth forward bar-by-bar using standard Wilder/EMA multipliers
            SELECT r.symbol, r._file_date, r.close_price, r.rn, r.gain, r.loss,
                (e.avg_gain * 13.0 + r.gain) / 14.0 as avg_gain,
                (e.avg_loss * 13.0 + r.loss) / 14.0 as avg_loss,
                (r.close_price * (2.0 / 51.0)) + (e.ema_val * (49.0 / 51.0)) as ema_val
            FROM baselines r
            INNER JOIN indicators_recursive e 
                ON r.symbol = e.symbol 
               AND r.rn = e.rn + 1
        ),
        indicators_final AS (
            SELECT symbol, _file_date, close_price, rn,
                ROUND(CASE 
                    WHEN avg_loss = 0 THEN 100
                    ELSE 100 - (100 / (1 + (avg_gain / avg_loss)))
                END, 2) as rsi,
                ROUND(ema_val, 2) as "50w_Moving_avg"
            FROM indicators_recursive
        ),
        price_stats AS (
            -- Dynamically fetch the breakout window high directly from the calculated dataset
            SELECT i.symbol, i._file_date, i.close_price, i."50w_Moving_avg", i.rsi,
                MAX(i.close_price) OVER (
                    PARTITION BY i.symbol 
                    ORDER BY i._file_date 
                    ROWS BETWEEN 26 PRECEDING AND 1 PRECEDING
                ) as prev_180d_friday_high
            FROM indicators_final i
        )
        -- Final Selection: Filter strictly for anchor Friday and enforce momentum bounds
        SELECT 
            p.symbol, 
            p._file_date, 
            p.close_price, 
            p.prev_180d_friday_high,
            p."50w_Moving_avg",
            CASE 
                WHEN p.close_price > p.prev_180d_friday_high THEN '✅ YES' 
                ELSE '❌ NO' 
            END as IS_BREAKOUT,
            p.rsi
        FROM price_stats p
        WHERE p._file_date = (SELECT ref_date FROM anchor_friday)
          AND p.rsi > 50.0
        ORDER BY p.symbol ASC;
    """)
    print("✅ Silver layer: Price Breakout Check Completed.")
    print("✅ Silver layer: 50-week EMA Calculation Completed.")
    print("✅ Silver layer: RSI Calculation Completed.")

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
    print("✅ Silver layer: volume_surge_check complete.")

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
    print("✅ Silver layer: delivery_weekly_check complete.")

    con.close()