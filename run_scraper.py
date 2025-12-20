import os, time, json, gspread, requests
from datetime import date

# ---------------- CONFIG ---------------- #
# This matches the 'Technicals' page. 
# Use "" for 1-Day, "|1" for 1-Min, "|5" for 5-Min, "|60" for 1-Hour
TF = "|1" 

STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

# ---------------- AUTH ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    client = gspread.service_account_from_dict(json.loads(creds_json)) if creds_json else gspread.service_account(filename="credentials.json")
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:]
except Exception as e:
    print(f"❌ Connection Error: {e}"); raise

current_date = date.today().strftime("%m/%d/%Y")

def get_exact_tv_values(symbol):
    """Hits the same internal API that the Technicals Tab uses"""
    # Try India scanner first (for 20MICRONS, etc.), then Global
    scanners = ["india", "global"]
    
    # These are the 14 columns usually found in the Technicals 'Oscillators' and 'MAs' tables
    columns = [
        f"RSI{TF}", f"Stoch.K{TF}", f"CCI20{TF}", f"ADX{TF}", f"AO{TF}", f"Mom{TF}", f"MACD.macd{TF}",
        f"EMA10{TF}", f"SMA10{TF}", f"EMA20{TF}", f"SMA20{TF}", f"EMA50{TF}", f"SMA50{TF}", f"EMA100{TF}"
    ]

    for scan in scanners:
        url = f"https://scanner.tradingview.com/{scan}/scan"
        # We try both NSE and US exchange formats
        tickers = [f"NSE:{symbol.upper()}", f"NASDAQ:{symbol.upper()}", f"NYSE:{symbol.upper()}"]
        
        payload = {"symbols": {"tickers": tickers}, "columns": columns}
        
        try:
            res = requests.post(url, json=payload, timeout=10).json()
            if "data" in res and len(res["data"]) > 0:
                raw_values = res["data"][0]["d"]
                # Clean and round exactly like the TV UI
                return [str(round(v, 2)) if isinstance(v, (int, float)) else "—" for v in raw_values]
        except:
            continue
    return ["N/A"] * 14

# ---------------- MAIN LOOP ---------------- #
print(f"🚀 Starting Exact-Match Scrape (Timeframe: {TF if TF else 'Daily'})...")

for i, row in enumerate(data_rows):
    name = row[0].strip()
    target_row = i + 2 # Matches your original mapping
    
    print(f"🔎 [{i+1}] {name}...", end=" ", flush=True)
    
    vals = get_exact_tv_values(name)
    
    # Build final row: [Name, Date, Value1, Value2, ... Value14]
    final_data = [name, current_date] + vals
    
    try:
        dest_sheet.update(f"A{target_row}", [final_data])
        print(f"✅ (Value: {vals[0]})")
    except Exception as e:
        print(f"❌ Write Error: {e}")

    # Very fast delay - API is robust
    time.sleep(0.1)

print("🏁 Process Complete.")
