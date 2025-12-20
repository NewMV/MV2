import os, time, json, gspread
from datetime import date
from tradingview_screener import Query, Column

# ---------------- CONFIG (EXACTLY AS PER YOUR CODE) ---------------- #
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

print(f"🔧 Range: {START_INDEX}-{END_INDEX} | Resume: {last_i}")

# ---------------- GOOGLE SHEETS AUTH (EXACTLY AS PER YOUR CODE) ---------------- #
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

# ---------------- NEW FAST SCREENER FETCH ---------------- #
def fetch_tradingview_data(symbols):
    """
    Replaces Selenium 'scrape_tradingview'. 
    Fetches the 14 technical values directly from TradingView's servers.
    """
    try:
        # These 14 fields represent the values you see in the chart 'valuesWrapper'
        fields = [
            'open', 'high', 'low', 'close',   # Price 1-4
            'volume', 'change',               # 5-6
            'RSI', 'MACD.macd', 'MACD.signal',# 7-9
            'EMA10', 'EMA20', 'SMA50',        # 10-12
            'SMA200', 'Mom'                   # 13-14
        ]
        
        # Set market to 'india' to find NSE/BSE stocks correctly
        q = (Query()
             .set_markets('india') 
             .select('name', *fields)
             .where(Column('name').isin(symbols)))
        
        count, df = q.get_scanner_data()
        
        results = {}
        if not df.empty:
            for _, row in df.iterrows():
                # Extract values skipping ticker/name columns (first 2)
                results[row['name']] = [str(val) if val is not None else "N/A" for val in row.values[2:]]
        return results
    except Exception as e:
        print(f"  ❌ API Error: {e}")
        return {}

# ---------------- MAIN LOOP (YOUR PROVEN LOGIC) ---------------- #
batch = []
batch_start = None
processed = success_count = 0

print(f"\n🚀 Processing Symbols via Screener API → 14 columns each")

for i, row in enumerate(data_rows):
    if i < last_i or i < START_INDEX or i > END_INDEX:
        continue
    
    name = row[0].strip().upper()
    target_row = i + 2
    
    if batch_start is None:
        batch_start = target_row
    
    print(f"[{i+1:4d}] {name} -> Row {target_row}")
    
    # Get values for this symbol
    api_results = fetch_tradingview_data([name])
    vals = api_results.get(name, ["N/A"] * 14)
    
    row_data = [name, current_date] + vals
    
    if any(v != "N/A" for v in vals):
        success_count += 1
    
    batch.append(row_data)
    processed += 1
    
    # YOUR PROVEN BATCH WRITE LOGIC
    if len(batch) >= 5:
        try:
            dest_sheet.update(f"A{batch_start}", batch)
            print(f"💾 Saved rows {batch_start} to {target_row}")
            batch = []
            batch_start = None
            time.sleep(1.5) # Gentle delay for GSheets API
        except Exception as e:
            print(f"❌ Write error: {e}")
    
    # YOUR PROVEN CHECKPOINT LOGIC
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(i + 1))

# Final flush
if batch and batch_start:
    try:
        dest_sheet.update(f"A{batch_start}", batch)
    except Exception as e:
        print(f"❌ Final write error: {e}")

print(f"\n🎉 COMPLETE! Processed: {processed} | Success: {success_count}")
