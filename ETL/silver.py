import duckdb

def load_silver(db_path):
    """
    Silver Stage: 
    1. Daily Cleaning: Strictly 11 columns. 
       Swapped SERIES, DATE1, NO_OF_TRADES for OPEN, HIGH, LOW for pattern detection.
    2. Multi-Timeframe Analytics: Weekly/6M averages.
    """
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    
    # 1. DAILY CLEAN TABLE (OHLC + Volume + Delivery)
    con.execute("DROP TABLE IF EXISTS silver.bhavcopy_clean;")
    con.execute("""
        CREATE TABLE silver.bhavcopy_clean (
            symbol          VARCHAR NOT NULL,
            open_price      DECIMAL(12,2),
            high_price      DECIMAL(12,2),
            low_price       DECIMAL(12,2),
            close_price     DECIMAL(12,2),
            prev_close      DECIMAL(12,2),
            TTL_TRD_QNTY    BIGINT,
            Delv_Per        DECIMAL(6,2),
            _source_file    VARCHAR,
            _loaded_at      TIMESTAMP,
            _file_date      DATE,
            PRIMARY KEY (symbol, _file_date)
        );
    """)

    print("🥈 Syncing 11-column Daily Silver layer with OHLC data...")
    con.execute("""
        INSERT INTO silver.bhavcopy_clean
        SELECT 
            TRIM(SYMBOL), 
            CAST(NULLIF(TRIM(OPEN_PRICE), '') AS DECIMAL(12,2)),
            CAST(NULLIF(TRIM(HIGH_PRICE), '') AS DECIMAL(12,2)),
            CAST(NULLIF(TRIM(LOW_PRICE), '') AS DECIMAL(12,2)),
            CAST(NULLIF(TRIM(CLOSE_PRICE), '') AS DECIMAL(12,2)),
            CAST(NULLIF(TRIM(PREV_CLOSE), '') AS DECIMAL(12,2)),
            CAST(NULLIF(TRIM(TTL_TRD_QNTY), '') AS BIGINT),
            CAST(NULLIF(TRIM(DELIV_PER), '') AS DECIMAL(6,2)),
            _source_file, now(), _file_date
        FROM bronze.bhavcopy_raw
        WHERE TRIM(SERIES) = 'EQ' AND _file_date IS NOT NULL
        ON CONFLICT DO NOTHING;
    """)

    # 2. ANALYTICS TABLE (Retaining existing logic)
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver.volume_multi_timeframe (
            symbol                VARCHAR NOT NULL,
            week_start            DATE NOT NULL,
            AVG_WLY_TTL_TRD_QNTY  DECIMAL(18,2),
            "6M_WLY_TTL_TRD_QNTY" DECIMAL(18,2),
            AVG_WLY_DELIV_PER     DECIMAL(6,2),
            "6M_WLY_DELIV_PER"    DECIMAL(6,2),
            AVG_WLY_CLOSE_PRICE   DECIMAL(12,2),
            "6M_WLY_CLOSE_PRICE"  DECIMAL(12,2),
            PRIMARY KEY (symbol, week_start)
        );
    """)

    print("📊 Computing Multi-Timeframe Stats...")
    con.execute("""
        INSERT INTO silver.volume_multi_timeframe (
            symbol, week_start, AVG_WLY_TTL_TRD_QNTY, "6M_WLY_TTL_TRD_QNTY", 
            AVG_WLY_DELIV_PER, "6M_WLY_DELIV_PER", AVG_WLY_CLOSE_PRICE, "6M_WLY_CLOSE_PRICE"
        )
        WITH weekly_base AS (
            SELECT 
                symbol,
                date_trunc('week', _file_date)::DATE as week_start,
                AVG(TTL_TRD_QNTY) as AVG_WLY_TTL_TRD_QNTY,
                AVG(Delv_Per) as AVG_WLY_DELIV_PER,
                AVG(close_price) as AVG_WLY_CLOSE_PRICE
            FROM silver.bhavcopy_clean
            GROUP BY 1, 2
        )
        SELECT 
            symbol, week_start, AVG_WLY_TTL_TRD_QNTY,
            AVG(AVG_WLY_TTL_TRD_QNTY) OVER(PARTITION BY symbol ORDER BY week_start ROWS BETWEEN 25 PRECEDING AND CURRENT ROW),
            AVG_WLY_DELIV_PER,
            AVG(AVG_WLY_DELIV_PER) OVER(PARTITION BY symbol ORDER BY week_start ROWS BETWEEN 25 PRECEDING AND CURRENT ROW),
            AVG_WLY_CLOSE_PRICE,
            AVG(AVG_WLY_CLOSE_PRICE) OVER(PARTITION BY symbol ORDER BY week_start ROWS BETWEEN 25 PRECEDING AND CURRENT ROW)
        FROM weekly_base
        ON CONFLICT (symbol, week_start) DO UPDATE SET
            AVG_WLY_TTL_TRD_QNTY = EXCLUDED.AVG_WLY_TTL_TRD_QNTY,
            "6M_WLY_TTL_TRD_QNTY" = EXCLUDED."6M_WLY_TTL_TRD_QNTY",
            AVG_WLY_DELIV_PER = EXCLUDED.AVG_WLY_DELIV_PER,
            "6M_WLY_DELIV_PER" = EXCLUDED."6M_WLY_DELIV_PER",
            AVG_WLY_CLOSE_PRICE = EXCLUDED.AVG_WLY_CLOSE_PRICE,
            "6M_WLY_CLOSE_PRICE" = EXCLUDED."6M_WLY_CLOSE_PRICE";
    """)
    con.close()
    print("✅ Silver layer complete.")