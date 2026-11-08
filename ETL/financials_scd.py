import duckdb
import requests
from bs4 import BeautifulSoup
import re
import hashlib
import time
from datetime import date, datetime

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
}

def clean_num(text):
    if not text or text.strip() in ['-', '', 'NA', 'NaN']:
        return None
    val = re.sub(r'[^\d.-]', '', text.strip())
    try:
        return float(val)
    except ValueError:
        return None

def normalize_label(text):
    """Strips expandable '+' and '-' buttons, special characters, and excess spaces."""
    cleaned = re.sub(r'[^a-zA-Z0-9% ]', ' ', text)
    return re.sub(r'\s+', ' ', cleaned).strip().lower()

def compute_hash(*args):
    raw_str = "|".join(str(a) for a in args)
    return hashlib.md5(raw_str.encode('utf-8')).hexdigest()

def parse_col_date(label):
    """Converts labels like 'Sep 2026' or 'Mar 2025' to a real date for chronological sorting."""
    parts = label.strip().split()
    if len(parts) == 2:
        month_str, yr_str = parts[0][:3].title(), parts[1]
        try:
            return datetime.strptime(f"{month_str} {yr_str}", "%b %Y").date()
        except Exception:
            pass
    return date(1970, 1, 1)

def scrape_full_screener(symbol):
    """Pulls stock fundamentals, statements, peers, and trades from Screener."""
    # Always fetch the primary canonical company page first
    url = f"https://www.screener.in/company/{symbol}/"
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        if res.status_code != 200:
            # Fallback to consolidated slug if required
            url = f"https://www.screener.in/company/{symbol}/consolidated/"
            res = requests.get(url, headers=HEADERS, timeout=15)
            if res.status_code != 200:
                return None
    except Exception:
        return None

    soup = BeautifulSoup(res.text, 'html.parser')

    # Verification: If the page has no top-ratios list (e.g. ETF or empty shell), skip it
    if not soup.select('ul#top-ratios li'):
        # Check if consolidated page has it
        try:
            alt_res = requests.get(f"https://www.screener.in/company/{symbol}/consolidated/", headers=HEADERS, timeout=15)
            if alt_res.status_code == 200:
                alt_soup = BeautifulSoup(alt_res.text, 'html.parser')
                if alt_soup.select('ul#top-ratios li'):
                    soup = alt_soup
        except Exception:
            pass

    # 1. Top Ratios & Direct Page Metrics
    ratios = {}
    for li in soup.select('ul#top-ratios li'):
        name = li.select_one('span.name')
        val = li.select_one('span.number') or li.select_one('span.nowrap')
        if name and val:
            norm_k = normalize_label(name.text)
            ratios[norm_k] = clean_num(val.text)

    cmp = ratios.get('current price')
    mcap = ratios.get('market cap')
    pe = ratios.get('stock p e') or ratios.get('stock pe')
    roce = ratios.get('roce')
    div_yield = ratios.get('dividend yield') or 0.0
    book_val = ratios.get('book value')
    pb = (cmp / book_val) if (cmp and book_val and book_val > 0) else None
    pb_by_pe = round(pb / pe, 4) if (pb and pe and pe > 0) else None

    # Helper: Normalizing Table Extractor
    def extract_table(section_id):
        sec = soup.select_one(f'section#{section_id}')
        if not sec: return {}, []
        table = sec.select_one('table.data-table')
        if not table: return {}, []
        ths = [th.text.strip() for th in table.select('thead tr th')[1:]]
        data = {}
        for tr in table.select('tbody tr'):
            tds = tr.select('td')
            if not tds: continue
            k = normalize_label(tds[0].text)
            vals = [clean_num(td.text) for td in tds[1:]]
            data[k] = vals
        return data, ths

    q_data, q_cols = extract_table('quarters')
    pnl_data, pnl_cols = extract_table('profit-loss')
    bs_data, bs_cols = extract_table('balance-sheet')
    sh_data, sh_cols = extract_table('shareholding')

    # Helper: Match line items regardless of button variations
    def get_row(data_dict, pattern):
        for k, v in data_dict.items():
            if pattern in k:
                return v
        return []

    # Balance Sheet Items
    borrowings_series = get_row(bs_data, 'borrowing')
    borrowings = borrowings_series[-1] if borrowings_series else 0.0
    
    # Liquid Cash from Cash Flows or Balance Sheet
    cf_data, _ = extract_table('cash-flow')
    net_cf_series = get_row(cf_data, 'net cash flow')
    cash_equiv = net_cf_series[-1] if net_cf_series else None

    # 2. Last 5 Quarters Construction (Chronological by date)
    quarters = []
    if q_cols:
        target_q_indices = list(range(len(q_cols)))[-5:]
        sales_q = get_row(q_data, 'sales')
        exp_q = get_row(q_data, 'expenses')
        op_q = get_row(q_data, 'operating profit')
        opm_q = get_row(q_data, 'opm')
        pbt_q = get_row(q_data, 'profit before tax')
        np_q = get_row(q_data, 'net profit')
        eps_q = get_row(q_data, 'eps')

        prom_q = get_row(sh_data, 'promoter')
        fii_q = get_row(sh_data, 'fii')
        dii_q = get_row(sh_data, 'dii')
        pub_q = get_row(sh_data, 'public')
        pledge_q = get_row(sh_data, 'pledged')

        for idx in target_q_indices:
            lbl = q_cols[idx]
            dt = parse_col_date(lbl)
            
            # Map corresponding shareholding index if date matches
            sh_idx = sh_cols.index(lbl) if lbl in sh_cols else -1

            quarters.append({
                "quarter_label": lbl,
                "period_date": dt,
                "sales": sales_q[idx] if idx < len(sales_q) else None,
                "expenses": exp_q[idx] if idx < len(exp_q) else None,
                "op": op_q[idx] if idx < len(op_q) else None,
                "opm": opm_q[idx] if idx < len(opm_q) else None,
                "pbt": pbt_q[idx] if idx < len(pbt_q) else None,
                "net_profit": np_q[idx] if idx < len(np_q) else None,
                "eps": eps_q[idx] if idx < len(eps_q) else None,
                "promoter": prom_q[sh_idx] if (sh_idx != -1 and sh_idx < len(prom_q)) else None,
                "fii": fii_q[sh_idx] if (sh_idx != -1 and sh_idx < len(fii_q)) else None,
                "dii": dii_q[sh_idx] if (sh_idx != -1 and sh_idx < len(dii_q)) else None,
                "public": pub_q[sh_idx] if (sh_idx != -1 and sh_idx < len(pub_q)) else None,
                "pledged": pledge_q[sh_idx] if (sh_idx != -1 and sh_idx < len(pledge_q)) else 0.0
            })
        # Sort chronologically
        quarters.sort(key=lambda x: x['period_date'])

    # 3. Last 5 Years Annual P&L Construction
    annual_pnl = []
    if pnl_cols:
        target_yr_indices = list(range(len(pnl_cols)))[-5:]
        sales_yr = get_row(pnl_data, 'sales')
        exp_yr = get_row(pnl_data, 'expenses')
        op_yr = get_row(pnl_data, 'operating profit')
        opm_yr = get_row(pnl_data, 'opm')
        pbt_yr = get_row(pnl_data, 'profit before tax')
        np_yr = get_row(pnl_data, 'net profit')
        eps_yr = get_row(pnl_data, 'eps')
        div_yr = get_row(pnl_data, 'dividend payout')

        for idx in target_yr_indices:
            lbl = pnl_cols[idx]
            annual_pnl.append({
                "year_label": lbl,
                "period_date": parse_col_date(lbl),
                "sales": sales_yr[idx] if idx < len(sales_yr) else None,
                "expenses": exp_yr[idx] if idx < len(exp_yr) else None,
                "op": op_yr[idx] if idx < len(op_yr) else None,
                "opm": opm_yr[idx] if idx < len(opm_yr) else None,
                "pbt": pbt_yr[idx] if idx < len(pbt_yr) else None,
                "net_profit": np_yr[idx] if idx < len(np_yr) else None,
                "eps": eps_yr[idx] if idx < len(eps_yr) else None,
                "dividend_payout": div_yr[idx] if idx < len(div_yr) else 0.0
            })
        annual_pnl.sort(key=lambda x: x['period_date'])

    # 4. Last 5 Years Annual Shareholding (Filter March / Annual Closings)
    annual_sh = []
    if sh_cols:
        march_indices = [i for i, h in enumerate(sh_cols) if 'mar' in h.lower()]
        target_sh_indices = march_indices[-5:] if len(march_indices) >= 5 else list(range(len(sh_cols)))[-5:]

        prom_yr = get_row(sh_data, 'promoter')
        fii_yr = get_row(sh_data, 'fii')
        dii_yr = get_row(sh_data, 'dii')
        pub_yr = get_row(sh_data, 'public')
        pledge_yr = get_row(sh_data, 'pledged')

        for idx in target_sh_indices:
            lbl = sh_cols[idx]
            annual_sh.append({
                "year_label": lbl,
                "period_date": parse_col_date(lbl),
                "promoter": prom_yr[idx] if idx < len(prom_yr) else None,
                "fii": fii_yr[idx] if idx < len(fii_yr) else None,
                "dii": dii_yr[idx] if idx < len(dii_yr) else None,
                "public": pub_yr[idx] if idx < len(pub_yr) else None,
                "pledged": pledge_yr[idx] if idx < len(pledge_yr) else 0.0
            })
        annual_sh.sort(key=lambda x: x['period_date'])

    # 5. Fair Values & Solvency
    eps_series = get_row(pnl_data, 'eps')
    eps_ttm = eps_series[-1] if len(eps_series) >= 1 else (quarters[-1]['eps'] if quarters else None)
    eps_last_year = eps_series[-2] if len(eps_series) >= 2 else eps_ttm
    eps_preceding = eps_series[-3] if len(eps_series) >= 3 else eps_last_year

    eps_fair_val = None
    if eps_preceding and eps_last_year and eps_ttm:
        eps_fair_val = round((eps_preceding + eps_last_year + eps_ttm) / 3.0, 2)

    stock_fair_val = None
    if eps_fair_val and pe and pe > 0 and cmp:
        stock_fair_val = round((eps_fair_val / pe) * cmp, 2)

    peg_ratio = ratios.get('peg ratio') or 1.0
    peg_fair_val = round(cmp / peg_ratio, 2) if cmp and peg_ratio and peg_ratio > 0 else None

    # TTM Net Profit calculation
    net_profit_ttm = get_row(pnl_data, 'net profit')[-1] if get_row(pnl_data, 'net profit') else (quarters[-1].get('net_profit') if quarters else None)
    debt_to_profit = round(borrowings / net_profit_ttm, 2) if (borrowings and net_profit_ttm and net_profit_ttm > 0) else 0.0
    debt_to_equity = ratios.get('debt to equity') or (round(borrowings / (mcap / (pb if pb else 1)), 2) if borrowings and mcap else 0.0)

    # Prioritize Screener's official ratios
    promoter_holding = ratios.get('promoter holding') or (quarters[-1].get('promoter') if quarters else None)
    pledged_percent = ratios.get('pledged percentage') or (quarters[-1].get('pledged') if quarters else 0.0)
    altman_z = ratios.get('altman z score') or 5.0
    piotroski = int(ratios.get('piotroski score') or 5)

    # 6. Extract Peers via Screener's internal API
    peer_symbols = []
    company_info = soup.select_one('#company-info') or soup.find(attrs={'data-warehouse-id': True})
    warehouse_id = company_info.get('data-warehouse-id') if company_info else None  

    if warehouse_id:
        try:
            peers_url = f"https://www.screener.in/api/company/{warehouse_id}/peers/"
            peers_res = requests.get(peers_url, headers=HEADERS, timeout=12)
            if peers_res.status_code == 200:
                peers_soup = BeautifulSoup(peers_res.text, 'html.parser')
                # Extract peer company tickers from table links
                for a in peers_soup.select('table.data-table tbody tr td a'):
                    href = a.get('href', '')
                    parts = [p for p in href.split('/') if p]
                    if len(parts) >= 2 and parts[0] == 'company':
                        peer_sym = parts[1].upper()
                        if peer_sym != symbol and peer_sym not in peer_symbols:
                            peer_symbols.append(peer_sym)
        except Exception as e:
            print(f"  Failed fetching peer table for {symbol}: {e}")

   # 7. Extract High Volume Deals (Insider Trades, SAST, Bulk/Block Deals)
    trades = []
    
    # Method A: Fetch directly from Screener's internal SAST/Trades API
    if warehouse_id:
        try:
            sast_url = f"https://www.screener.in/api/company/{warehouse_id}/sast/"
            sast_res = requests.get(sast_url, headers=HEADERS, timeout=12)
            if sast_res.status_code == 200:
                sast_soup = BeautifulSoup(sast_res.text, 'html.parser')
                for tr in sast_soup.select('table.data-table tbody tr'):
                    tds = tr.select('td')
                    if len(tds) >= 4:
                        dt_raw = tds[0].text.strip()
                        try:
                            # Handles formats like '05 Nov 2026' or 'Nov 2026'
                            t_date = datetime.strptime(dt_raw, '%d %b %Y').date()
                        except Exception:
                            try:
                                t_date = datetime.strptime(dt_raw, '%b %Y').date()
                            except Exception:
                                t_date = date.today()
                                
                        client = tds[1].text.strip()
                        raw_deal = tds[2].text.strip().lower()
                        deal_type = 'BUY' if any(w in raw_deal for w in ['buy', 'acqui', 'market purchase']) else 'SELL'
                        qty = int(clean_num(tds[3].text) or 0)
                        price = clean_num(tds[4].text) if len(tds) >= 5 else 0.0
                        
                        trade_id = compute_hash(symbol, dt_raw, client, qty, price)
                        trades.append({
                            "trade_id": trade_id,
                            "symbol": symbol,
                            "trade_date": t_date,
                            "client_name": client,
                            "deal_type": deal_type,
                            "quantity": qty,
                            "trade_price": price or 0.0,
                            "source_category": "INSIDER_OR_BULK"
                        })
        except Exception as e:
            print(f"  Note: Sast API trade check bypassed for {symbol}: {e}")

    # Method B: Fallback to HTML table under section#shareholding or section#deals
    if not trades:
        deals_sec = soup.select_one('section#deals') or soup.select_one('div#trades')
        if deals_sec:
            for tr in deals_sec.select('table.data-table tbody tr'):
                tds = tr.select('td')
                if len(tds) >= 4:
                    dt_raw = tds[0].text.strip()
                    try:
                        t_date = datetime.strptime(dt_raw, '%b %Y').date()
                    except Exception:
                        t_date = date.today()
                    client = tds[1].text.strip()
                    deal_type = 'BUY' if 'buy' in tds[2].text.strip().lower() else 'SELL'
                    qty = int(clean_num(tds[3].text) or 0)
                    price = clean_num(tds[4].text) if len(tds) >= 5 else 0.0
                    trade_id = compute_hash(symbol, dt_raw, client, qty, price)
                    trades.append({
                        "trade_id": trade_id,
                        "symbol": symbol,
                        "trade_date": t_date,
                        "client_name": client,
                        "deal_type": deal_type,
                        "quantity": qty,
                        "trade_price": price or 0.0,
                        "source_category": "INSIDER_OR_BULK"
                    })

    return {
        "symbol": symbol, "cmp": cmp, "mcap": mcap, "pe": pe, "roce": roce,
        "eps_ttm": eps_ttm, "debt_to_profit": debt_to_profit, "dividend_yield": div_yield,
        "debt_to_equity": debt_to_equity, "promoter_holding": promoter_holding,
        "pledged_percent": pledged_percent or 0.0, "pb_by_pe": pb_by_pe,
        "cash_equivalents": cash_equiv, "altman_z": altman_z, "piotroski": piotroski,
        "eps_fair_val": eps_fair_val, "peg_fair_val": peg_fair_val, "stock_fair_val": stock_fair_val,
        "quarters": quarters, "annual_pnl": annual_pnl, "annual_sh": annual_sh,
        "peer_symbols": peer_symbols[:6],
        "trades": trades
    }

def init_fundamentals_schema(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS fundamentals;")
    
    # Auto-migration check: If old schema without period_date exists, recreate tables
    try:
        cols = [c[0] for c in con.execute("DESCRIBE fundamentals.quarterly_scd").fetchall()]
        if 'period_date' not in cols:
            con.execute("DROP TABLE IF EXISTS fundamentals.quarterly_scd CASCADE;")
            con.execute("DROP TABLE IF EXISTS fundamentals.annual_pnl_scd CASCADE;")
            con.execute("DROP TABLE IF EXISTS fundamentals.annual_shareholding_scd CASCADE;")
            con.execute("DROP TABLE IF EXISTS fundamentals.valuation_scd CASCADE;")
            con.execute("DROP TABLE IF EXISTS fundamentals.peer_comparison_scd CASCADE;")
    except Exception:
        pass

    con.execute("""
        CREATE TABLE IF NOT EXISTS fundamentals.valuation_scd (
            scd_id VARCHAR PRIMARY KEY, symbol VARCHAR NOT NULL, as_of_quarter VARCHAR NOT NULL,
            cmp DOUBLE, market_cap_cr DOUBLE, pe DOUBLE, roce DOUBLE, eps_ttm DOUBLE, debt_to_profit DOUBLE,
            dividend_yield DOUBLE, debt_to_equity DOUBLE, promoter_holding DOUBLE, pledged_percent DOUBLE,
            pb_by_pe DOUBLE, cash_equivalents_cr DOUBLE, altman_z_score DOUBLE, piotroski_score INT,
            eps_fair_value DOUBLE, peg_fair_value DOUBLE, stock_fair_value DOUBLE, row_hash VARCHAR NOT NULL,
            version INT NOT NULL DEFAULT 1, valid_from DATE NOT NULL, valid_to DATE,
            is_current BOOLEAN NOT NULL DEFAULT TRUE, created_at TIMESTAMP DEFAULT now()
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fundamentals.quarterly_scd (
            scd_id VARCHAR PRIMARY KEY, symbol VARCHAR NOT NULL, quarter_label VARCHAR NOT NULL,
            period_date DATE NOT NULL, sales_cr DOUBLE, expenses_cr DOUBLE,
            operating_profit_cr DOUBLE, opm_percent DOUBLE, profit_before_tax_cr DOUBLE,
            net_profit_cr DOUBLE, eps DOUBLE, promoter_holding_pct DOUBLE, fii_holding_pct DOUBLE,
            dii_holding_pct DOUBLE, public_holding_pct DOUBLE, pledged_pct DOUBLE, row_hash VARCHAR NOT NULL,
            version INT NOT NULL DEFAULT 1, valid_from DATE NOT NULL, valid_to DATE,
            is_current BOOLEAN NOT NULL DEFAULT TRUE, created_at TIMESTAMP DEFAULT now()
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fundamentals.annual_pnl_scd (
            scd_id VARCHAR PRIMARY KEY, symbol VARCHAR NOT NULL, year_label VARCHAR NOT NULL,
            period_date DATE NOT NULL, sales_cr DOUBLE, expenses_cr DOUBLE, operating_profit_cr DOUBLE,
            opm_percent DOUBLE, profit_before_tax_cr DOUBLE, net_profit_cr DOUBLE, eps DOUBLE,
            dividend_payout_pct DOUBLE, row_hash VARCHAR NOT NULL, version INT NOT NULL DEFAULT 1,
            valid_from DATE NOT NULL, valid_to DATE, is_current BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT now()
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fundamentals.annual_shareholding_scd (
            scd_id VARCHAR PRIMARY KEY, symbol VARCHAR NOT NULL, year_label VARCHAR NOT NULL,
            period_date DATE NOT NULL, promoter_holding_pct DOUBLE, fii_holding_pct DOUBLE, dii_holding_pct DOUBLE,
            public_holding_pct DOUBLE, pledged_pct DOUBLE, row_hash VARCHAR NOT NULL,
            version INT NOT NULL DEFAULT 1, valid_from DATE NOT NULL, valid_to DATE,
            is_current BOOLEAN NOT NULL DEFAULT TRUE, created_at TIMESTAMP DEFAULT now()
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fundamentals.peer_comparison_scd (
            scd_id VARCHAR PRIMARY KEY, target_symbol VARCHAR NOT NULL, peer_symbol VARCHAR NOT NULL,
            peer_name VARCHAR, cmp DOUBLE, market_cap_cr DOUBLE, pe DOUBLE, roce DOUBLE, eps_ttm DOUBLE,
            debt_to_profit DOUBLE, dividend_yield DOUBLE, debt_to_equity DOUBLE, promoter_holding DOUBLE,
            pledged_percent DOUBLE, pb_by_pe DOUBLE, cash_equivalents_cr DOUBLE, altman_z_score DOUBLE,
            piotroski_score INT, peg_fair_value DOUBLE, eps_fair_value DOUBLE, stock_fair_value DOUBLE,
            row_hash VARCHAR NOT NULL, version INT NOT NULL DEFAULT 1, valid_from DATE NOT NULL,
            valid_to DATE, is_current BOOLEAN NOT NULL DEFAULT TRUE, created_at TIMESTAMP DEFAULT now()
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fundamentals.high_volume_trades (
            trade_id VARCHAR PRIMARY KEY, symbol VARCHAR NOT NULL, trade_date DATE, client_name VARCHAR,
            deal_type VARCHAR, quantity BIGINT, trade_price DOUBLE, source_category VARCHAR,
            created_at TIMESTAMP DEFAULT now()
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fundamentals.company_insights (
            symbol VARCHAR PRIMARY KEY, valuation_category VARCHAR, institutional_sentiment VARCHAR,
            audit_integrity_status VARCHAR, health_score INT, health_rating VARCHAR,
            fair_value_discount_pct DOUBLE, fii_trend_qoq DOUBLE, restatements_count INT,
            key_observations VARCHAR, updated_at TIMESTAMP DEFAULT now()
        );
    """)
    con.execute("""
        CREATE OR REPLACE VIEW fundamentals.v_quarterly_revisions AS
        WITH version_pairs AS (
            SELECT 
                cur.symbol, cur.quarter_label, cur.version AS current_version, prev.version AS prior_version,
                prev.sales_cr AS prior_sales, cur.sales_cr AS restated_sales,
                ROUND(cur.sales_cr - prev.sales_cr, 2) AS sales_diff_cr,
                prev.net_profit_cr AS prior_net_profit, cur.net_profit_cr AS restated_net_profit,
                ROUND(cur.net_profit_cr - prev.net_profit_cr, 2) AS net_profit_diff_cr,
                cur.valid_from AS restated_date
            FROM fundamentals.quarterly_scd cur
            INNER JOIN fundamentals.quarterly_scd prev 
                ON cur.symbol = prev.symbol AND cur.quarter_label = prev.quarter_label 
                AND cur.version = prev.version + 1
            WHERE cur.is_current = TRUE
        )
        SELECT *, CASE 
            WHEN net_profit_diff_cr < 0 THEN 'REVISED_DOWN'
            WHEN net_profit_diff_cr > 0 THEN 'REVISED_UP'
            ELSE 'UNCHANGED'
        END AS revision_direction FROM version_pairs;
    """)

def run_scd_sync_engine(db_path):
    con = duckdb.connect(db_path)
    init_fundamentals_schema(con)
    symbols = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM gold.historical_signals").fetchall()]
    today = date.today()

    for sym in symbols:
        time.sleep(1)
        data = scrape_full_screener(sym)
        if not data: continue

        # --- A. Valuation SCD Type 2 ---
        as_of = data['quarters'][-1]['quarter_label'] if data['quarters'] else 'LATEST'
        val_hash = compute_hash(
            data['cmp'], data['pe'], data['roce'], data['eps_ttm'], data['debt_to_profit'],
            data['debt_to_equity'], data['eps_fair_val'], data['stock_fair_val'], data['altman_z'], data['piotroski']
        )
        existing_val = con.execute("""
            SELECT scd_id, row_hash, version FROM fundamentals.valuation_scd 
            WHERE symbol = ? AND is_current = TRUE
        """, [sym]).fetchone()

        if existing_val is None:
            con.execute("""
                INSERT INTO fundamentals.valuation_scd VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, NULL, TRUE, now()
                )
            """, [
                f"{sym}_val_v1", sym, as_of, data['cmp'], data['mcap'], data['pe'], data['roce'],
                data['eps_ttm'], data['debt_to_profit'], data['dividend_yield'], data['debt_to_equity'],
                data['promoter_holding'], data['pledged_percent'], data['pb_by_pe'], data['cash_equivalents'],
                data['altman_z'], data['piotroski'], data['eps_fair_val'], data['peg_fair_val'], data['stock_fair_val'],
                val_hash, today
            ])
        elif existing_val[1] != val_hash:
            old_id, _, old_v = existing_val
            con.execute("UPDATE fundamentals.valuation_scd SET is_current = FALSE, valid_to = ? WHERE scd_id = ?", [today, old_id])
            con.execute("""
                INSERT INTO fundamentals.valuation_scd VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, TRUE, now()
                )
            """, [
                f"{sym}_val_v{old_v + 1}", sym, as_of, data['cmp'], data['mcap'], data['pe'], data['roce'],
                data['eps_ttm'], data['debt_to_profit'], data['dividend_yield'], data['debt_to_equity'],
                data['promoter_holding'], data['pledged_percent'], data['pb_by_pe'], data['cash_equivalents'],
                data['altman_z'], data['piotroski'], data['eps_fair_val'], data['peg_fair_val'], data['stock_fair_val'],
                val_hash, old_v + 1, today
            ])

        # --- B. Last 5 Quarters SCD Type 2 ---
        for q in data['quarters']:
            qlbl = q['quarter_label']
            q_hash = compute_hash(q.get('sales'), q.get('op'), q.get('net_profit'), q.get('eps'), q.get('promoter'), q.get('pledged'))
            existing_q = con.execute("""
                SELECT scd_id, row_hash, version FROM fundamentals.quarterly_scd
                WHERE symbol = ? AND quarter_label = ? AND is_current = TRUE
            """, [sym, qlbl]).fetchone()

            if existing_q is None:
                con.execute("""
                    INSERT INTO fundamentals.quarterly_scd (
                        scd_id, symbol, quarter_label, period_date, sales_cr, expenses_cr,
                        operating_profit_cr, opm_percent, profit_before_tax_cr, net_profit_cr,
                        eps, promoter_holding_pct, fii_holding_pct, dii_holding_pct,
                        public_holding_pct, pledged_pct, row_hash, version, valid_from,
                        valid_to, is_current, created_at
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, NULL, TRUE, now()
                    )
                """, [
                    f"{sym}_{qlbl.replace(' ', '_')}_v1", sym, qlbl, q.get('period_date'),
                    q.get('sales'), q.get('expenses'), q.get('op'), q.get('opm'), q.get('pbt'), q.get('net_profit'),
                    q.get('eps'), q.get('promoter'), q.get('fii'), q.get('dii'), q.get('public'), q.get('pledged'),
                    q_hash, today
                ])
            elif existing_q[1] != q_hash:
                old_qid, _, old_qv = existing_q
                con.execute("UPDATE fundamentals.quarterly_scd SET is_current = FALSE, valid_to = ? WHERE scd_id = ?", [today, old_qid])
                con.execute("""
                    INSERT INTO fundamentals.quarterly_scd (
                        scd_id, symbol, quarter_label, period_date, sales_cr, expenses_cr,
                        operating_profit_cr, opm_percent, profit_before_tax_cr, net_profit_cr,
                        eps, promoter_holding_pct, fii_holding_pct, dii_holding_pct,
                        public_holding_pct, pledged_pct, row_hash, version, valid_from,
                        valid_to, is_current, created_at
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, TRUE, now()
                    )
                """, [
                    f"{sym}_{qlbl.replace(' ', '_')}_v{old_qv + 1}", sym, qlbl, q.get('period_date'),
                    q.get('sales'), q.get('expenses'), q.get('op'), q.get('opm'), q.get('pbt'), q.get('net_profit'),
                    q.get('eps'), q.get('promoter'), q.get('fii'), q.get('dii'), q.get('public'), q.get('pledged'),
                    q_hash, old_qv + 1, today
                ])

        # --- C. Last 5 Years Annual P&L SCD Type 2 ---
        for y in data['annual_pnl']:
            ylbl = y['year_label']
            y_hash = compute_hash(y.get('sales'), y.get('op'), y.get('net_profit'), y.get('eps'), y.get('dividend_payout'))
            existing_y = con.execute("""
                SELECT scd_id, row_hash, version FROM fundamentals.annual_pnl_scd
                WHERE symbol = ? AND year_label = ? AND is_current = TRUE
            """, [sym, ylbl]).fetchone()

            if existing_y is None:
                con.execute("""
                    INSERT INTO fundamentals.annual_pnl_scd VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, NULL, TRUE, now()
                    )
                """, [
                    f"{sym}_{ylbl.replace(' ', '_')}_v1", sym, ylbl, y.get('period_date'), y.get('sales'), y.get('expenses'),
                    y.get('op'), y.get('opm'), y.get('pbt'), y.get('net_profit'), y.get('eps'),
                    y.get('dividend_payout'), y_hash, today
                ])
            elif existing_y[1] != y_hash:
                old_yid, _, old_yv = existing_y
                con.execute("UPDATE fundamentals.annual_pnl_scd SET is_current = FALSE, valid_to = ? WHERE scd_id = ?", [today, old_yid])
                con.execute("""
                    INSERT INTO fundamentals.annual_pnl_scd VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, TRUE, now()
                    )
                """, [
                    f"{sym}_{ylbl.replace(' ', '_')}_v{old_yv + 1}", sym, ylbl, y.get('period_date'), y.get('sales'), y.get('expenses'),
                    y.get('op'), y.get('opm'), y.get('pbt'), y.get('net_profit'), y.get('eps'),
                    y.get('dividend_payout'), y_hash, old_yv + 1, today
                ])

        # --- D. Last 5 Years Shareholding SCD Type 2 ---
        for sh in data['annual_sh']:
            sh_lbl = sh['year_label']
            sh_hash = compute_hash(sh.get('promoter'), sh.get('fii'), sh.get('dii'), sh.get('pledged'))
            existing_sh = con.execute("""
                SELECT scd_id, row_hash, version FROM fundamentals.annual_shareholding_scd
                WHERE symbol = ? AND year_label = ? AND is_current = TRUE
            """, [sym, sh_lbl]).fetchone()

            if existing_sh is None:
                con.execute("""
                    INSERT INTO fundamentals.annual_shareholding_scd VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, NULL, TRUE, now()
                    )
                """, [
                    f"{sym}_sh_{sh_lbl.replace(' ', '_')}_v1", sym, sh_lbl, sh.get('period_date'),
                    sh.get('promoter'), sh.get('fii'), sh.get('dii'), sh.get('public'), sh.get('pledged'),
                    sh_hash, today
                ])
            elif existing_sh[1] != sh_hash:
                old_shid, _, old_shv = existing_sh
                con.execute("UPDATE fundamentals.annual_shareholding_scd SET is_current = FALSE, valid_to = ? WHERE scd_id = ?", [today, old_shid])
                con.execute("""
                    INSERT INTO fundamentals.annual_shareholding_scd VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, TRUE, now()
                    )
                """, [
                    f"{sym}_sh_{sh_lbl.replace(' ', '_')}_v{old_shv + 1}", sym, sh_lbl, sh.get('period_date'),
                    sh.get('promoter'), sh.get('fii'), sh.get('dii'), sh.get('public'), sh.get('pledged'),
                    sh_hash, old_shv + 1, today
                ])

        # --- E. High Volume Trades Log ---
        for t in data['trades']:
            con.execute("""
                INSERT OR IGNORE INTO fundamentals.high_volume_trades VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, now()
                )
            """, [
                t["trade_id"], t["symbol"], t["trade_date"], t["client_name"],
                t["deal_type"], t["quantity"], t["trade_price"], t["source_category"]
            ])

        # --- F. Peer Comparison Expansion ---
        for p_sym in data['peer_symbols']:
            try:
                time.sleep(0.75)
                p_data = scrape_full_screener(p_sym)
                if not p_data: continue
                p_hash = compute_hash(p_data['cmp'], p_data['pe'], p_data['roce'], p_data['stock_fair_val'])
                
                existing_p = con.execute("""
                    SELECT scd_id, row_hash, version FROM fundamentals.peer_comparison_scd
                    WHERE target_symbol = ? AND peer_symbol = ? AND is_current = TRUE
                """, [sym, p_sym]).fetchone()

                if existing_p is None:
                    con.execute("""
                        INSERT INTO fundamentals.peer_comparison_scd VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, NULL, TRUE, now()
                        )
                    """, [
                        f"{sym}_{p_sym}_v1", sym, p_sym, p_sym, p_data['cmp'], p_data['mcap'], p_data['pe'],
                        p_data['roce'], p_data['eps_ttm'], p_data['debt_to_profit'], p_data['dividend_yield'],
                        p_data['debt_to_equity'], p_data['promoter_holding'], p_data['pledged_percent'],
                        p_data['pb_by_pe'], p_data['cash_equivalents'], p_data['altman_z'], p_data['piotroski'],
                        p_data['peg_fair_val'], p_data['eps_fair_val'], p_data['stock_fair_val'],
                        p_hash, today
                    ])
                elif existing_p[1] != p_hash:
                    old_pid, _, old_pv = existing_p
                    con.execute("UPDATE fundamentals.peer_comparison_scd SET is_current = FALSE, valid_to = ? WHERE scd_id = ?", [today, old_pid])
                    con.execute("""
                        INSERT INTO fundamentals.peer_comparison_scd VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, TRUE, now()
                        )
                    """, [
                        f"{sym}_{p_sym}_v{old_pv + 1}", sym, p_sym, p_sym, p_data['cmp'], p_data['mcap'], p_data['pe'],
                        p_data['roce'], p_data['eps_ttm'], p_data['debt_to_profit'], p_data['dividend_yield'],
                        p_data['debt_to_equity'], p_data['promoter_holding'], p_data['pledged_percent'],
                        p_data['pb_by_pe'], p_data['cash_equivalents'], p_data['altman_z'], p_data['piotroski'],
                        p_data['peg_fair_val'], p_data['eps_fair_val'], p_data['stock_fair_val'],
                        p_hash, old_pv + 1, today
                    ])
            except Exception as e:
                print(f"Error indexing peer {p_sym} for {sym}: {e}")

        # --- G. Insights Engine ---
        obs = []
        cmp_v = data.get('cmp') or 0.0
        fv_v = data.get('stock_fair_val') or data.get('peg_fair_val') or 0.0
        disc = round(((fv_v - cmp_v) / fv_v) * 100, 2) if (cmp_v > 0 and fv_v > 0) else 0.0
        
        val_cat = "UNDERVALUED" if disc >= 15 else ("EXPENSIVE" if disc <= -15 else "FAIRLY VALUED")
        obs.append(f"{val_cat} ({disc}% vs fair value).")

        fii_trend = 0.0
        if len(data['quarters']) >= 2:
            q_cur, q_prv = data['quarters'][-1], data['quarters'][-2]
            fii_trend = round((q_cur.get('fii') or 0.0) - (q_prv.get('fii') or 0.0), 2)
        inst_sentiment = "ACCUMULATION" if fii_trend > 0.5 else ("DISTRIBUTION" if fii_trend < -0.5 else "NEUTRAL")
        obs.append(f"Institutional FII {inst_sentiment} ({fii_trend}% QoQ).")

        restate_cnt = con.execute("SELECT COUNT(*) FROM fundamentals.quarterly_scd WHERE symbol = ? AND version > 1", [sym]).fetchone()[0]
        audit_stat = "CLEAN" if restate_cnt == 0 else "RESTATED"

        score = 0
        if (data.get('roce') or 0) >= 18: score += 25
        if (data.get('debt_to_equity') or 0) <= 0.5: score += 25
        if data.get('piotroski') and data['piotroski'] >= 7: score += 25
        if val_cat == "UNDERVALUED": score += 25
        rating = "PRIME" if score >= 75 else ("MODERATE" if score >= 50 else "SPECULATIVE")

        con.execute("""
            INSERT OR REPLACE INTO fundamentals.company_insights VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now()
            )
        """, [
            sym, val_cat, inst_sentiment, audit_stat, score, rating,
            disc, fii_trend, restate_cnt, " ".join(obs)
        ])

        print(f"  ✓ Finished complete SCD extraction for: {sym}")

    con.close()