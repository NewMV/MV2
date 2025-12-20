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
except Exception as e:
    print(f"❌ Connection Error: {e}"); raise

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- DATA MAPPING ---------------- #
def fetch_batch_data(symbols):
    """
    Fetches the EXACT 14 technical values found in the chart status line.
    """
    try:
        # These fields correspond to what you see in the 'valuesWrapper'
        fields = [
            'close', 'volume',          # Price/Vol
            'recommendation',           # The 'Strong Buy/Sell' text
            'RSI',                      # RSI(14)
            'MACD.macd', 'MACD.signal', # MACD lines
            'EMA10', 'EMA20', 'EMA50',  # Short/Mid EMAs
            'SMA100', 'SMA200',         # Long term MAs
            'Stoch.K', 'Stoch.D',       # Stochastic
            'change'                    # Day change %
        ]
        
        q = (Query()
             .set_markets('india') 
             .select('name', *fields)
             .where(Column('name').isin(symbols)))
        
        count, df = q.get_scanner_data()
        
        results = {}
        if not df.empty:
            for _, row in df.iterrows():
                # Skip first two (ticker/name), keep the rest
                data = [str(val) if val is not None else "N/A" for val in row.values[2:]]
                results[row['name']] = data
        return results
    except Exception as e:
        print(f"❌ API Error: {e}")
        return {}

# ---------------- MAIN LOOP ---------------- #
batch_size = 5
for i in range(last_i, min(len(data_rows), END_INDEX + 1), batch_size):
    chunk = data_rows[i : i + batch_size]
    symbols = [r[0].strip().upper() for r in chunk if r[0]]
    
    print(f"🔎 Fetching {len(symbols)} symbols from India Market...")
    api_data = fetch_batch_data(symbols)
    
    final_rows = []
    for row in chunk:
        name = row[0].strip().upper()
        # Ensure we always have 14 columns of data + name + date
        vals = api_data.get(name, ["N/A"] * 14)
        final_rows.append([name, current_date] + vals)
    
    try:
        dest_sheet.update(f"A{i + 2}", final_rows)
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(str(i + batch_size))
        print(f"💾 Saved rows {i+2} to {i+2+len(final_rows)-1}")
    except Exception as e:
        print(f"❌ Write Error: {e}")
    
    time.sleep(1.2)

print("\n🏁 Process Finished.")
