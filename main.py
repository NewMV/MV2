import os
import time
import json
import gspread
from datetime import date
from tradingview_screener import Query, Column

# ---------------- CONFIG (KEPT EXACTLY THE SAME) ---------------- #
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
    except:
        pass

print(f"🔧 Range: {START_INDEX}-{END_INDEX} | Resume: {last_i}")

# ---------------- GOOGLE SHEETS (KEPT EXACTLY THE SAME) ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    if creds_json:
        client = gspread.service_account_from_dict(json.loads(creds_json))
    else:
        client = gspread.service_account(filename="credentials.json")
        
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:]
    print(f"✅ Connected. Processing {END_INDEX-START_INDEX+1} symbols")
except Exception as e:
    print(f"❌ Connection Error: {e}")
    raise

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- NEW API FETCH LOGIC (FAST REPLACEMENT) ---------------- #
def fetch_tradingview_data(symbols):
    """Replaces the 14-column Selenium scraper with one API call."""
    try:
        # TradingView API needs symbols like "NASDAQ:AAPL" or "NSE:RELIANCE"
        # If your sheet only has "AAPL", we default to a general scan
        q = Query().select(
            'close', 'volume', 'RSI', 'MACD.macd', 'MACD.signal', 
            'open', 'high', 'low', 'EMA10', 'EMA20', 'SMA50', 'SMA200', 'Mom', 'change'
        ).where(
            Column('name').isin(symbols)
        )
        
        count, df = q.get_scanner_data()
        
        # Create a mapping dictionary { Symbol: [Values] }
        result_map = {}
        if not df.empty:
            for _, row in df.iterrows():
                # Extract values in order (skipping the index/ticker)
                result_map[row['name']] = [str(val) for val in list(row.values)[1:]]
        return result_map
    except Exception as e:
        print(f"  ❌ API Error: {e}")
        return {}

# ---------------- MAIN LOOP (STRUCTURE KEPT) ---------------- #
batch = []
batch_start = None
processed = 0

# We process in chunks of 50 to take advantage of the API speed
for i in range(last_i, min(len(data_rows), END_INDEX + 1)):
    # Group names for the API (5 symbols at a time to match your original batch write)
    current_row = data_rows[i]
    name = current_row[0].strip()
    target_row = i + 2
    
    if batch_start is None:
        batch_start = target_row

    print(f"[{i+1:4d}] Fetching: {name}")
    
    # In this logic, we call the API for the single name to keep your 
    # exact "batch of 5" writing logic intact.
    api_data = fetch_tradingview_data([name])
    vals = api_data.get(name, ["N/A"] * 14)
    
    row_data = [name, current_date] + vals
    batch.append(row_data)
    processed += 1

    # Batch write (Every 5 rows as per your original script)
    if len(batch) >= 5:
        try:
            dest_sheet.update(f"A{batch_start}", batch)
            print(f"💾 Saved Rows {batch_start}-{target_row}")
            batch = []
            batch_start = None
            # Update Checkpoint
            with open(CHECKPOINT_FILE, "w") as f:
                f.write(str(i + 1))
            time.sleep(1) # Small delay to be safe with Google API
        except Exception as e:
            print(f"❌ Write error: {e}")

# Final flush
if batch and batch_start:
    dest_sheet.update(f"A{batch_start}", batch)

print(f"\n🎉 COMPLETE! Processed: {processed}")
