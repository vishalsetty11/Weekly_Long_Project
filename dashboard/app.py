from flask import Flask, render_template, jsonify
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

def get_db_connection():
    return duckdb.connect(DB_PATH, read_only=True)

def safe_fetch_all(cursor):
    columns = [d[0] for d in cursor.description]
    results = []
    while True:
        row = cursor.fetchone()
        if row is None: break
        formatted_row = {columns[i]: (row[i].isoformat() if isinstance(row[i], (datetime.date, datetime.datetime)) else row[i]) for i in range(len(columns))}
        results.append(formatted_row)
    return results

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/get_history')
def get_history():
    try:
        con = get_db_connection()
        query = """
        SELECT 
            _file_date,
            symbol,
            close_price, 
            prev_180d_friday_high,
            "3X_180dvol_MULTIPLE",
            IS_3X_SURGE,
            IS_BREAKOUT AS IS_PRICE_BREAKOUT
        FROM gold.historical_signals
        ORDER BY "3X_180dvol_MULTIPLE" DESC
        """
        results = safe_fetch_all(con.execute(query))
        con.close()
        return jsonify(results)
    except Exception as e:
        print("!!! ERROR IN get_history !!!")
        print(e) 
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)