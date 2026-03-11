def load_gold(db_path):
    """
    Gold Stage:
    1. Marubozu Pattern Detection: Identifies Bullish/Bearish patterns for the LATEST day.
    2. Hammer Pattern Detection: Identifies Valid Hammer candles for the LATEST day.
    3. 4-Week Strategy History: Tracks stocks passing Volume & Delivery filters for the last 28 days.
    """
    import duckdb
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

    # --- 1. MARUBOZU SIGNALS (Current Day Only) ---
    con.execute("DROP TABLE IF EXISTS gold.marubozu_signals;")
    con.execute("""
        CREATE TABLE gold.marubozu_signals (
            symbol          VARCHAR NOT NULL,
            signal_date     DATE NOT NULL,
            pattern_type    VARCHAR, -- 'BULLISH' or 'BEARISH'
            open_price      DECIMAL(12,1),
            close_price     DECIMAL(12,1),
            price_chg_pct   DECIMAL(6,1),
            PRIMARY KEY (symbol, signal_date)
        );
    """)

    print("🥇 Identifying Marubozu patterns for current day...")
    con.execute("""
        INSERT INTO gold.marubozu_signals
        SELECT 
            symbol,
            _file_date,
            CASE 
                WHEN open_price = low_price AND close_price = high_price THEN 'BULLISH'
                WHEN open_price = high_price AND close_price = low_price THEN 'BEARISH'
            END as pattern_type,
            ROUND(open_price, 1),
            ROUND(close_price, 1),
            ROUND(((close_price - prev_close)/NULLIF(prev_close,0))*100, 1) as price_chg_pct
        FROM silver.bhavcopy_clean
        WHERE _file_date = (SELECT MAX(_file_date) FROM silver.bhavcopy_clean)
          AND (
            (open_price = low_price AND close_price = high_price) -- Bullish: Open=Low, Close=High
            OR 
            (open_price = high_price AND close_price = low_price) -- Bearish: Open=High, Close=Low
          )
        ON CONFLICT DO NOTHING;
    """)

    # --- 2. HAMMER SIGNALS (Current Day Only) ---
    # PRECISE FIX: Implementing formulas from provided image
    con.execute("DROP TABLE IF EXISTS gold.hammer_signals;")
    con.execute("""
        CREATE TABLE gold.hammer_signals (
            symbol          VARCHAR NOT NULL,
            signal_date     DATE NOT NULL,
            open_price      DECIMAL(12,1),
            high_price      DECIMAL(12,1),
            low_price       DECIMAL(12,1),
            close_price     DECIMAL(12,1),
            PRIMARY KEY (symbol, signal_date)
        );
    """)

    print("🥇 Identifying Hammer patterns for current day...")
    con.execute("""
        INSERT INTO gold.hammer_signals
        SELECT 
            symbol,
            _file_date,
            ROUND(open_price, 1),
            ROUND(high_price, 1),
            ROUND(low_price, 1),
            ROUND(close_price, 1)
        FROM silver.bhavcopy_clean
        WHERE _file_date = (SELECT MAX(_file_date) FROM silver.bhavcopy_clean)
          AND (high_price - low_price) > 0  -- Ensure not a flat line
          -- 1. Lower Wick >= 2 * BodyLength
          AND (LEAST(open_price, close_price) - low_price) >= (2 * ABS(close_price - open_price))
          -- 2. Upper Wick < 0.5 * BodyLength
          AND (high_price - GREATEST(open_price, close_price)) < (0.5 * ABS(close_price - open_price))
          -- 3. Body Position: Min(Open, Close) >= Low + 0.75 * Range
          AND LEAST(open_price, close_price) >= (low_price + (0.75 * (high_price - low_price)))
        ON CONFLICT DO NOTHING;
    """)

    # --- 3. STRATEGY SIGNALS (Rolling 4-Week History) ---
    con.execute("""
        CREATE TABLE IF NOT EXISTS gold.weekly_long (
            symbol                VARCHAR NOT NULL,
            signal_date           DATE NOT NULL,
            prev_close            DECIMAL(12,1),
            close_price           DECIMAL(12,1),
            weekly_avg_vol        DECIMAL(18,1),
            six_month_avg_vol     DECIMAL(18,1),
            weekly_avg_deliv      DECIMAL(6,1),
            six_month_avg_deliv   DECIMAL(6,1),
            weekly_avg_price      DECIMAL(12,1),
            six_month_avg_price   DECIMAL(12,1),
            _computed_at          TIMESTAMP,
            PRIMARY KEY (symbol, signal_date)
        );
    """)

    print("🥇 Identifying 3x Volume & Momentum signals for rolling 4-week window...")
    con.execute("""
        INSERT INTO gold.weekly_long
        SELECT 
            c.symbol, 
            c._file_date, 
            ROUND(c.prev_close, 1), 
            ROUND(c.close_price, 1),
            ROUND(v.AVG_WLY_TTL_TRD_QNTY, 1), 
            ROUND(v."6M_WLY_TTL_TRD_QNTY", 1),
            ROUND(v.AVG_WLY_DELIV_PER, 1), 
            ROUND(v."6M_WLY_DELIV_PER", 1),
            ROUND(v.AVG_WLY_CLOSE_PRICE, 1), 
            ROUND(v."6M_WLY_CLOSE_PRICE", 1),
            now()
        FROM silver.bhavcopy_clean c
        JOIN silver.volume_multi_timeframe v 
          ON c.symbol = v.symbol 
          AND date_trunc('week', c._file_date)::DATE = v.week_start
        WHERE 
            v.AVG_WLY_TTL_TRD_QNTY >= (3 * v."6M_WLY_TTL_TRD_QNTY")
            AND v.AVG_WLY_DELIV_PER >= v."6M_WLY_DELIV_PER"
            AND v.AVG_WLY_CLOSE_PRICE > v."6M_WLY_CLOSE_PRICE"
            AND c._file_date >= (SELECT MAX(_file_date) FROM silver.bhavcopy_clean) - INTERVAL 28 DAYS
        ON CONFLICT (symbol, signal_date) DO NOTHING;
    """)

    # Cleanup: Remove signals older than 28 days to maintain strictly 4 weeks of data
    con.execute("""
        DELETE FROM gold.weekly_long 
        WHERE signal_date < (SELECT MAX(signal_date) FROM gold.weekly_long) - INTERVAL 28 DAYS
    """)

    con.close()
    print("✅ Gold Layer updated with Marubozu, Hammer, and historical Weekly Long strategy signals.")