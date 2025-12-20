import os
import time
import json
import gspread
from datetime import date
from tradingview_screener import Query, Column

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint.txt")

# Resume from checkpoint logic
last_i = START_INDEX
if os.path.exists(CHECKPOINT_FILE):
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            last_i = int(f.read().strip())
    except: pass

# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    client = gspread.service_account_from_dict(json.loads(creds_json)) if creds_json else gspread.service_account(filename="credentials.json")
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:]
    print(f"✅ Connected. Processing {END_INDEX-START_INDEX+1} symbols")
except Exception as e:
    print(f"❌ Connection Error: {e}"); raise

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- FIXED API DATA FETCH ---------------- #
def fetch_batch_data(symbol_names):
    """Fetches data for multiple symbols at once to avoid 'N/A' shifts."""
    try:
        # Define the 14 indicators you need
        indicators = [
            'close', 'volume', 'RSI', 'MACD.macd', 'MACD.signal', 
            'open', 'high', 'low', 'EMA10', 'EMA20', 'SMA50', 'SMA200', 'Mom', 'change'
        ]
        
        # Build Query: select 'name' + indicators
        q = Query().select('name', *indicators).where(Column('name').isin(symbol_names))
        
        count, df = q.get_scanner_data()
        
        results = {}
        if not df.empty:
            for _, row in df.iterrows():
                # Extracting values starting after 'ticker' and 'name' columns
                # row.values[0] is ticker, row.values[1] is name
                data_points = [str(val) if val is not None else "N/A" for val in row.values[2:]]
                results[row['name']] = data_points
        return results
    except Exception as e:
        print(f"  ❌ API Error: {e}")
        return {}

# ---------------- MAIN LOOP (BATCH PROCESSING) ---------------- #
batch_size = 5
for i in range(last_i, min(len(data_rows), END_INDEX + 1), batch_size):
    # Get symbols for this batch
    current_batch_rows = data_rows[i : i + batch_size]
    symbols = [row[0].strip().upper() for row in current_batch_rows]
    
    print(f"🚀 Processing: {symbols}")
    
    # Single API call for the whole batch
    api_results = fetch_batch_data(symbols)
    
    upload_data = []
    for row in current_batch_rows:
        name = row[0].strip().upper()
        # Retrieve values or fill with N/A if missing from API response
        vals = api_results.get(name, ["N/A"] * 14)
        upload_data.append([name, current_date] + vals)
    
    # Write to Sheet
    try:
        dest_sheet.update(f"A{i + 2}", upload_data)
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(str(i + batch_size))
        print(f"💾 Saved rows {i+2} to {i+2+len(upload_data)-1}")
    except Exception as e:
        print(f"❌ Write error: {e}")
    
    time.sleep(1.5) # Prevent rate limiting

print("\n🏁 Process finished.")
