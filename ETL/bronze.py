from datetime import datetime
import io


def load_bronze(db_path):
    """
    Bronze Stage: Parses CSV content and applies strict 'Stocks Only' filters.
    Filters: 
    1. Series: Only EQ, BE, SM, ST, BZ (Equity/SME Stock types).
    2. Symbols: Excludes ETFs, Funds, and Indices using expanded pattern matching.
    """
    import pandas as pd
    import duckdb
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.bhavcopy_raw (
            SYMBOL VARCHAR, SERIES VARCHAR, DATE1 VARCHAR, PREV_CLOSE VARCHAR,
            OPEN_PRICE VARCHAR, HIGH_PRICE VARCHAR, LOW_PRICE VARCHAR, LAST_PRICE VARCHAR,
            CLOSE_PRICE VARCHAR, AVG_PRICE VARCHAR, TTL_TRD_QNTY VARCHAR, TURNOVER_LACS VARCHAR,
            NO_OF_TRADES VARCHAR, DELIV_QTY VARCHAR, DELIV_PER VARCHAR,
            _source_file VARCHAR, _loaded_at TIMESTAMP, _file_date DATE
        );
    """)

    # Only process files not already in Bronze
    pending = con.execute("""
        SELECT filename, content FROM ingestedCSVData.raw_files 
        WHERE filename NOT IN (SELECT DISTINCT _source_file FROM bronze.bhavcopy_raw)
    """).fetchall()

    if not pending:
        print("⏩ No new data in 'ingestedCSVData' to parse.")
        con.close()
        return

    print(f"🥉 Filtering Stocks to Bronze ({len(pending)} files)...")
    import re

    # PRECISE FILTER CRITERIA
    VALID_SERIES = ['EQ', 'BE', 'SM', 'ST', 'BZ']
    
    # Expanded list of patterns found in ETFs/Funds/Indices
    # Added keywords like NIFTY, GILT, and specific tricky symbols like HEALTHY/CONSUMER
    EXCLUDE_PATTERNS = [
        'BEES$', 'ETF$', 'IETF$', 'CASE$', 'ADD$', 'GOLD$', 'SILVER$', 'LIQUID$', 
        'BETA$', 'GILT$', 'NIFTY$', 'SDL', 'ADD$', 'VALUE$', 'QUAL$', 'MOM$', 
        'ALPHA$', 'LOWVOL$', '^HEALTHY$', '^CONSUMER$', '^FINANCE$'
    ]
    exclude_regex = "|".join(EXCLUDE_PATTERNS)

    for filename, content in pending:
        try:
            date_part = filename.split('_')[-1].replace('.csv', '')
            file_date = datetime.strptime(date_part, '%d%m%Y').date()
            
            df = pd.read_csv(io.StringIO(content), dtype=str)
            df.columns = df.columns.str.strip()
            
            # 1. Clean data values
            df['SYMBOL'] = df['SYMBOL'].str.strip().str.upper()
            df['SERIES'] = df['SERIES'].str.strip().str.upper()
            
            # 2. Filter by SERIES (Equity/SME only)
            # This automatically removes GS (Govt Securities) and GB (Gold Bonds)
            df = df[df['SERIES'].isin(VALID_SERIES)].copy()
            
            # 3. Filter by SYMBOL pattern using Regex (No ETFs/Funds/Indices)
            # Use regex to catch suffixes and specific whole-word matches
            df = df[~df['SYMBOL'].str.contains(exclude_regex, regex=True, na=False)].copy()

            if df.empty:
                print(f"⚠️ No stocks found in {filename} after filtering.")
                continue

            # Add metadata
            df['_source_file'] = filename
            df['_loaded_at'] = datetime.now()
            df['_file_date'] = file_date
            
            con.execute("INSERT INTO bronze.bhavcopy_raw SELECT * FROM df")
            print(f"✅ Ingested {len(df)} stocks from {filename}")
            
        except Exception as e:
            print(f"❌ Error parsing {filename}: {e}")

    con.close()