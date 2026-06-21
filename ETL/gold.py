import duckdb

def load_gold(db_path):
    """
    Gold Stage: Stores weekly historical snapshots of all signals.
    Enables MM-YYYY filtering for dashboards.
    """
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

    con.execute("""
        CREATE OR REPLACE TABLE gold.historical_signals AS
        SELECT 
            vs.symbol, 
            vs._file_date, 
            vs.TTL_TRD_QNTY, 
            vs.avg_vol_180d, 
            ROUND(vs.TTL_TRD_QNTY/(NULLIF(vs.avg_vol_180d, 0) * 3), 2) AS "3X_180dvol_MULTIPLE",
            vs.IS_3X_SURGE,
            pb.close_price, 
            pb.rsi,
            pb."50w_Moving_avg",
            pb.prev_180d_friday_high,
            pb.IS_BREAKOUT
        FROM silver.volume_surge_check AS vs
        JOIN silver.price_breakout_check AS pb 
        ON vs.symbol = pb.symbol 
        AND vs._file_date = pb._file_date
        WHERE vs.IS_3X_SURGE = '✅ YES' 
        AND pb.IS_BREAKOUT = '✅ YES'
        AND vs._file_date >= CURRENT_DATE - INTERVAL '2 months'
        ORDER BY vs._file_date DESC;
    """)

    con.close()
    print("✅ Gold layer: Historical snapshots stored successfully.")