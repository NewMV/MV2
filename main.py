import os, time, json, gspread
from datetime import date
from tradingview_screener import Query, Column

# ---------------- CONFIG (SAME AS YOUR CODE) ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint.txt")

# Resume from checkpoint
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

# ---------------- THE RELIABLE API FETCH ---------------- #
def fetch_tradingview_data(symbols):
    """
    Directly queries the technical fields that appear in your old 
    'valuesWrapper' CSS selector.
    """
    try:
        # These 14 fields match the standard technical values of a chart status line
        fields = [
            'close', 'volume', 'RSI', 'MACD.macd', 'MACD.signal', 
            'open', 'high', 'low', 'EMA10', 'EMA20', 'SMA50', 'SMA200', 'Mom', 'change'
        ]
        
        q = (Query()
             .set_markets('india') # Targets NSE/BSE symbols
             .select('name', *fields)
             .where(Column('name').isin(symbols)))
        
        count, df = q.get_scanner_data()
        
        results = {}
        if not df.empty:
            for _, row in df.iterrows():
                # Extract values skipping the first two columns (ticker and name)
                # This ensures we get exactly the 14 columns you expect
                results[row['name']] = [str(val) if val is not None else "N/A" for val in row.values[2:]]
        return results
    except Exception as e:
        print(f"  ❌ API Error: {e}")
        return {}

# ---------------- MAIN LOOP (YOUR PROVEN LOGIC) ---------------- #
batch = []
batch_start = None

print(f"\n🚀 Processing Rows {START_INDEX+2} to {END_INDEX+2}")

# Process in chunks of 5 for your batch writing logic
for i in range(last_i, min(len(data_rows), END_INDEX + 1)):
    row = data_rows[i]
    name = row[0].strip().upper()
    target_row = i + 2
    
    if batch_start is None:
        batch_start = target_row

    print(f"🔎 [{i}] {name} -> Row {target_row}")

    # Use API for the single symbol to keep your exact loop logic
    api_data = fetch_tradingview_data([name])
    vals = api_data.get(name, ["N/A"] * 14)
    
    row_data = [name, current_date] + vals
    batch.append(row_data)

    # Your proven batch writing logic
    if len(batch) >= 5:
        try:
            dest_sheet.update(f"A{batch_start}", batch)
            print(f"💾 Saved rows {batch_start} to {target_row}")
            batch, batch_start = [], None
            time.sleep(1.5)
        except Exception as e:
            print(f"❌ Write Error: {e}")

    # Checkpoint
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(i + 1))

# Final flush
if batch and batch_start:
    dest_sheet.update(f"A{batch_start}", batch)

print("\n🏁 Process finished.")
