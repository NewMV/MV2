import os, time, json, gspread
from datetime import date
from tradingview_screener import Query, Column

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint.txt")

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
    print(f"✅ Connected. Range: {START_INDEX}-{END_INDEX}")
except Exception as e:
    print(f"❌ Connection Error: {e}"); raise

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- DATA FETCH & CLEANING ---------------- #
def fetch_and_clean_data(symbols):
    """Fetches data and rounds it to match your chart's visual values."""
    try:
        # These are the 14 standard technical indicators
        fields = [
            'open', 'high', 'low', 'close', 
            'volume', 'change', 'RSI', 'MACD.macd', 
            'MACD.signal', 'EMA10', 'EMA20', 'SMA50', 
            'SMA200', 'Mom'
        ]
        
        q = (Query()
             .set_markets('india') 
             .select('name', *fields)
             .where(Column('name').isin(symbols)))
        
        count, df = q.get_scanner_data()
        
        results = {}
        if not df.empty:
            for _, row in df.iterrows():
                clean_row = []
                # Skip first two internal columns (ticker, name)
                for val in row.values[2:]:
                    if val is None:
                        clean_row.append("N/A")
                    elif isinstance(val, (int, float)):
                        # This rounds 192.577834 to 192.58
                        clean_row.append(round(val, 2))
                    else:
                        clean_row.append(str(val))
                results[row['name']] = clean_row
        return results
    except Exception as e:
        print(f"  ❌ API Error: {e}")
        return {}

# ---------------- MAIN LOOP ---------------- #
batch_size = 5
for i in range(last_i, min(len(data_rows), END_INDEX + 1), batch_size):
    chunk = data_rows[i : i + batch_size]
    symbols = [r[0].strip().upper() for r in chunk if r[0]]
    
    if not symbols: continue
    
    print(f"🚀 Fetching: {symbols}")
    api_data = fetch_and_clean_data(symbols)
    
    upload_batch = []
    for row in chunk:
        name = row[0].strip().upper()
        # Get the 14 cleaned values or N/A
        vals = api_data.get(name, ["N/A"] * 14)
        upload_batch.append([name, current_date] + vals)
    
    # Write to Google Sheets
    try:
        dest_sheet.update(f"A{i + 2}", upload_batch)
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(str(i + len(upload_batch)))
        print(f"💾 Saved rows {i+2} to {i+2+len(upload_batch)-1}")
    except Exception as e:
        print(f"❌ Write Error: {e}")
    
    time.sleep(1.2)

print("\n🏁 All Data Processed Successfully.")
