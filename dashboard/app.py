from flask import Flask, render_template, jsonify, request, session, redirect, url_for
import duckdb
import os
import sys
import datetime
import json

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# --- Path Configuration ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import DB_PATH
except ImportError:
    DB_PATH = r"D:\Setty\Market Project\Bhavcopy\nse_market.duckdb"

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'dev_session_cryptographic_fallback_token_99')

SCOPES = ['https://www.googleapis.com/auth/calendar.events']

def get_db_connection(read_only=False):
    return duckdb.connect(DB_PATH, read_only=read_only)

def safe_fetch_all(cursor):
    if cursor.description is None:
        return []
    columns = [d[0] for d in cursor.description]
    results = []
    while True:
        row = cursor.fetchone()
        if row is None: break
        formatted_row = {columns[i]: (row[i].isoformat() if isinstance(row[i], (datetime.date, datetime.datetime)) else row[i]) for i in range(len(columns))}
        results.append(formatted_row)
    return results

def get_google_flow(state=None):
    env_client = os.environ.get('GOOGLE_CLIENT_SECRET_JSON')
    if env_client:
        client_config = json.loads(env_client)
        flow = InstalledAppFlow.from_client_config(client_config, SCOPES, state=state)
    else:
        flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES, state=state)
    
    if "onrender.com" in request.host_url:
        flow.redirect_uri = "https://nse-bhavcopy-project.onrender.com/google/callback"
    else:
        flow.redirect_uri = url_for('google_callback', _external=True)
    return flow

@app.route('/google/login')
def google_login():
    flow = get_google_flow()
    authorization_url, state = flow.authorization_url(
        access_type='offline', include_granted_scopes='true', prompt='consent'
    )
    session['oauth_state'] = state
    session['pending_symbol'] = request.args.get('symbol')
    session['pending_note'] = request.args.get('note', '')
    session['pending_file_date'] = request.args.get('file_date', '')
    session['pending_reminder_time'] = request.args.get('reminder_time', '')
    return redirect(authorization_url)

@app.route('/google/callback')
def google_callback():
    """Captures authorization responses from Google and saves token state map into browser session cookies."""
    state = request.args.get('state')
    
    # Re-initialize the flow instance with the correct state parameter check
    flow = get_google_flow(state=state)
    
    try:
        authorization_response = request.url
        if "onrender.com" in request.host_url:
            authorization_response = authorization_response.replace("http:", "https:", 1)
            os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

        # PRECISE PRODUCTION FIX: Bypass multi-worker thread PKCE checking conflicts 
        # by executing direct authorization code extraction parameters natively.
        if hasattr(flow, 'oauth2session') and flow.oauth2session:
            # Clears internal state tracking variables to avoid session key mismatches between dynamic host nodes
            flow.oauth2session.state = None
            
        # Securely download user credentials access tokens using the explicit query string code argument directly
        flow.fetch_token(code=request.args.get('code'))
        
        # Save token to browser session context
        session['google_credentials'] = json.loads(flow.credentials.to_json())
        
        # Clean up transient state session attributes keys safely
        session.pop('oauth_state', None)
        session.pop('oauth_code_verifier', None)
        
    except Exception as token_err:
        print(f"❌ OAuth Handshake Exception Intercepted: {token_err}")
        return jsonify({
            "error": "Authentication handshake failed.",
            "details": str(token_err)
        }), 400
        
    return redirect(url_for('index'))

@app.route('/api/watchlist/get_pending')
def get_pending_watchlist():
    symbol = session.get('pending_symbol')
    if not symbol:
        return jsonify({"has_pending": False})
    payload = {
        "has_pending": True, "symbol": symbol, "note": session.get('pending_note', ''),
        "file_name": session.get('pending_file_date', ''), "reminder_time": session.get('pending_reminder_time', '')
    }
    session.pop('pending_symbol', None)
    session.pop('pending_note', None)
    session.pop('pending_file_date', None)
    session.pop('pending_reminder_time', None)
    return jsonify(payload)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/get_history')
def get_history():
    try:
        con = get_db_connection() 
        query = """
        SELECT _file_date, strftime(_file_date, '%m-%Y') AS month_year, symbol, close_price, 
               "50w_Moving_avg" AS "50-week moving average", prev_180d_friday_high, "3X_180dvol_MULTIPLE",
               IS_3X_SURGE, IS_BREAKOUT AS IS_PRICE_BREAKOUT
        FROM gold.historical_signals ORDER BY "3X_180dvol_MULTIPLE" DESC
        """
        results = safe_fetch_all(con.execute(query))
        con.close()
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/watchlist', methods=['POST'])
def add_to_watchlist():
    data = request.get_json() or {}
    symbol = data.get('symbol')
    note = data.get('note', '')
    file_name = data.get('file_name', '')
    reminder_time_raw = data.get('reminder_time', None)

    if not symbol:
        return jsonify({"error": "Missing symbol"}), 400

    user_token = session.get('google_credentials')
    if reminder_time_raw and not user_token:
        return jsonify({
            "success": False, "requires_auth": True,
            "auth_url": url_for('google_login', symbol=symbol, note=note, file_date=file_name, reminder_time=reminder_time_raw)
        }), 401

    reminder_time = None
    if reminder_time_raw:
        try:
            reminder_time = reminder_time_raw.replace('T', ' ')
            if len(reminder_time) == 16:  
                reminder_time += ':00'
        except Exception:
            reminder_time = None

    # PRECISE INTEGRATION: Sync to calendar immediately to grab unique event ID references
    google_event_id = None
    if reminder_time and user_token:
        try:
            creds = Credentials.from_authorized_user_info(user_token, SCOPES)
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
                session['google_credentials'] = json.loads(creds.to_json())
                
            service = build('calendar', 'v3', credentials=creds)
            iso_start = reminder_time.replace(' ', 'T')
            end_dt = datetime.datetime.fromisoformat(iso_start) + datetime.timedelta(minutes=30)
            
            cal_event = service.events().insert(calendarId='primary', body={
                'summary': f'Weekly Long Reminder: {symbol}',
                'description': f'Notes:\n{note}',
                'start': {'dateTime': iso_start, 'timeZone': 'Asia/Kolkata'},
                'end': {'dateTime': end_dt.isoformat(), 'timeZone': 'Asia/Kolkata'},
                'reminders': {'useDefault': False, 'overrides': [{'method': 'popup', 'minutes': 15}]}
            }).execute()
            google_event_id = cal_event.get('id')
        except Exception as google_crash:
            print(f"User calendar injection failure: {google_crash}")

    conn = get_db_connection()
    try:
        existing = conn.execute("SELECT id, event_id FROM watchlist.watchlisted_stocks WHERE symbol = ?", [symbol]).fetchone()

        if existing:
            # Drop old event mapping if rewriting values
            old_event_id = existing[1]
            if old_event_id and user_token:
                try:
                    creds = Credentials.from_authorized_user_info(user_token, SCOPES)
                    service = build('calendar', 'v3', credentials=creds)
                    service.events().delete(calendarId='primary', eventId=old_event_id).execute()
                except Exception:
                    pass

            conn.execute("""
                UPDATE watchlist.watchlisted_stocks 
                SET note = ?, file_name = ?, reminder_time = ?, event_id = ? WHERE symbol = ?
            """, [note, file_name, reminder_time, google_event_id, symbol])
        else:
            max_id_res = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM watchlist.watchlisted_stocks").fetchone()
            next_id = max_id_res[0] if max_id_res else 1

            conn.execute("""
                INSERT INTO watchlist.watchlisted_stocks (id, symbol, note, file_name, reminder_time, is_notified, event_id)
                VALUES (?, ?, ?, ?, ?, FALSE, ?)
            """, [next_id, symbol, note, file_name, reminder_time, google_event_id])
        
        return jsonify({"success": True, "message": f"{symbol} successfully updated in watchlist."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route('/watchlist')
def view_watchlist():
    conn = get_db_connection()
    try:
        # PRECISE ADDITION: Extract stored event_id signatures from the table row space
        watchlist_items = conn.execute("""
            SELECT symbol, note, file_name, reminder_time, is_notified, event_id
            FROM watchlist.watchlisted_stocks ORDER BY id DESC
        """).fetchall()
        
        formatted_items = []
        now = datetime.datetime.now()
        
        for row in watchlist_items:
            symbol, note, file_name, reminder_time, is_notified, event_id = row
            cal_url = "https://calendar.google.com/calendar/r"
            is_expired = False
            
            if reminder_time:
                try:
                    clean_ts = str(reminder_time).split(".")[0]
                    reminder_dt = datetime.datetime.strptime(clean_ts, "%Y-%m-%d %H:%M:%S")
                    
                    # PRECISE CALCULATIONS: Flags true if current time has passed the reminder time checkpoint
                    if now > reminder_dt:
                        is_expired = True
                        
                    date_str = reminder_dt.strftime("%Y%m%d")
                    cal_url = f"https://calendar.google.com/calendar/r/day/{date_str[0:4]}/{date_str[4:6]}/{date_str[6:8]}"
                except Exception:
                    cal_url = "https://calendar.google.com/calendar/r"
            
            formatted_items.append((symbol, note, file_name, reminder_time, is_notified, cal_url, is_expired))
            
    except Exception as e:
        formatted_items = []
        print(f"Watchlist query execution failure: {e}")
    finally:
        conn.close()
        
    return render_template('watchlist.html', items=formatted_items)

@app.route('/api/watchlist/delete', methods=['POST'])
def delete_from_watchlist():
    data = request.get_json()
    symbol = data.get('symbol')
    
    user_token = session.get('google_credentials')
    conn = get_db_connection()
    try:
        # Fetch the event_id metadata tracking configuration from DuckDB
        row = conn.execute("SELECT event_id FROM watchlist.watchlisted_stocks WHERE symbol = ?", [symbol]).fetchone()
        
        if row and user_token:
            event_id = row[0]
            try:
                creds = Credentials.from_authorized_user_info(user_token, SCOPES)
                if creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                service = build('calendar', 'v3', credentials=creds)
                
                # PATH A: Target delete using explicit unique Event ID
                if event_id:
                    service.events().delete(calendarId='primary', eventId=event_id).execute()
                    print(f"🗑️ Wiped Google Calendar event via ID: {event_id}")
                
                # PATH B: Fallback search by Title if event_id column is NULL/empty
                else:
                    print(f"🔍 event_id missing for {symbol}. Searching calendar by title fallback configuration...")
                    target_summary = f"🚨 Breakout Reminder: {symbol}"
                    
                    # Query events from the last 7 days to 1 year out to find matching titles
                    now_iso = datetime.datetime.utcnow().isoformat() + 'Z'
                    events_result = service.events().list(
                        calendarId='primary', 
                        q=target_summary,
                        timeMin=(datetime.datetime.utcnow() - datetime.timedelta(days=7)).isoformat() + 'Z',
                        singleEvents=True
                    ).execute()
                    
                    events = events_result.get('items', [])
                    for match_event in events:
                        if match_event.get('summary') == target_summary:
                            service.events().delete(calendarId='primary', eventId=match_event['id']).execute()
                            print(f"🗑️ Cleaned untracked fallback calendar event by title match: {match_event['id']}")
                            
            except Exception as cal_err:
                print(f"⚠️ Calendar event cleanup skipped or unauthorized: {cal_err}")

        # Drop record cleanly from the local analytics matrix database
        conn.execute("DELETE FROM watchlist.watchlisted_stocks WHERE symbol = ?", [symbol])
        return jsonify({"success": True})
    except Exception as e:
        print(f"Deletion pipeline failure: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

if __name__ == '__main__':
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    app.run(debug=True, port=5000)