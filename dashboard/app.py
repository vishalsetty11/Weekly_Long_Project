from flask import Flask, render_template, request, jsonify
import duckdb
import os
import sys
import datetime

# --- Path Configuration ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import DB_PATH
except ImportError:
    DB_PATH = r"D:\Setty\Market Project\Bhavcopy\nse_market.duckdb"

app = Flask(__name__)
is_cloud = DB_PATH.startswith("md:")

def get_db_connection():
    if is_cloud:
        return duckdb.connect(DB_PATH)
    return duckdb.connect(DB_PATH, read_only=True)

def safe_fetch_all(cursor):
    """Formats DuckDB types into JSON serializable formats with 0.1 precision."""
    if not cursor or not cursor.description:
        return [], []
    columns = [d[0] for d in cursor.description]
    results = []
    while True:
        row = cursor.fetchone()
        if row is None: break
        formatted_row = {}
        for i, col in enumerate(columns):
            val = row[i]
            if isinstance(val, (datetime.date, datetime.datetime)):
                formatted_row[col] = val.isoformat()
            elif hasattr(val, '__float__') and not isinstance(val, (int, float)):
                formatted_row[col] = round(float(val), 1)
            elif isinstance(val, float):
                formatted_row[col] = round(val, 1)
            else:
                formatted_row[col] = val
        results.append(formatted_row)
    return results, columns

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/stats')
def get_stats():
    try:
        con = get_db_connection()
        res = con.execute("""
            SELECT 
                (SELECT count(*) FROM bronze.bhavcopy_raw),
                (SELECT count(*) FROM silver.bhavcopy_clean),
                (SELECT count(*) FROM gold.weekly_long),
                (SELECT max(_loaded_at) FROM silver.bhavcopy_clean)
        """).fetchone()
        stats = {
            "bronze": res[0], "silver": res[1], "gold": res[2],
            "last_update": res[3].strftime("%Y-%m-%d %H:%M") if res[3] else "N/A",
            "source": "MotherDuck" if is_cloud else "Local"
        }
        con.close()
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- Tab 1: Weekly Long (Rolling 4 Weeks) ---
@app.route('/api/weekly_long')
def get_weekly_long():
    try:
        con = get_db_connection()
        cursor = con.execute("SELECT * FROM gold.weekly_long ORDER BY signal_date DESC, symbol ASC")
        results, _ = safe_fetch_all(cursor)
        con.close()
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- Tab 2: Marubozu Patterns ---
@app.route('/api/marubozu')
def get_marubozu():
    try:
        con = get_db_connection()
        cursor = con.execute("SELECT * FROM gold.marubozu_signals ORDER BY signal_date DESC, pattern_type DESC")
        results, _ = safe_fetch_all(cursor)
        con.close()
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- Tab 3: Hammer Patterns ---
@app.route('/api/hammer')
def get_hammer():
    try:
        con = get_db_connection()
        cursor = con.execute("""
            SELECT h.*, ROUND(((h.close_price - c.prev_close)/NULLIF(c.prev_close,0))*100, 1) as price_chg_pct
            FROM gold.hammer_signals h
            JOIN silver.bhavcopy_clean c ON h.symbol = c.symbol AND h.signal_date = c._file_date
            ORDER BY signal_date DESC, symbol ASC
        """)
        results, _ = safe_fetch_all(cursor)
        con.close()
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/query', methods=['POST'])
def run_query():
    data = request.get_json()
    sql = data.get('sql', '').strip()
    if not sql: return jsonify({"error": "No SQL provided"}), 400
    try:
        con = get_db_connection()
        cursor = con.execute(sql)
        results, columns = safe_fetch_all(cursor)
        con.close()
        return jsonify({"results": results, "columns": columns})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)