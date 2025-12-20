import os, time, json, gspread, requests
from datetime import date

# ---------------- CONFIG ---------------- #
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

def get_tradingview_data_multi_market(symbol):
    """Checks India and Global scanners for the symbol data"""
    # Markets to check: 'india' for NSE/BSE, 'global' for US/Others
    markets = ["india", "global"]
    
    # These are the standard TV internal names for Technical columns
    columns = [
        "Recommend.All", "RSI", "Stoch.K", "Stoch.D", 
        "EMA10", "SMA10", "EMA20", "SMA20", 
        "EMA30", "SMA30", "EMA50", "SMA50", 
        "EMA100", "SMA100"
    ]

    for market in markets:
        url = f"https://scanner.tradingview.com/{market}/scan"
        
        # We try both just the symbol AND the exchange prefix
        tickers_to_try = [f"NSE:{symbol.upper()}", f"BSE:{symbol.upper()}", symbol.upper()]
        
        payload = {
            "symbols": {"tickers": tickers_to_try},
            "columns": columns
        }

        try:
            response = requests.post(url, json=payload, timeout=10)
            data = response.json()
            if "data" in data and len(data["data"]) > 0:
                # Find the first one that returned data
                values = data["data"][0]["d"]
                return [str(round(v, 2)) if isinstance(v, (int, float)) else "0.00" for v in values]
        except:
            continue
            
    return ["N/A"] * 14

# ---------------- MAIN LOOP ---------------- #
print(f"🚀 Starting Universal Scrape (Fixed for Indian Markets)...")

for i, row in enumerate(data_rows):
    raw_name = row[0].strip()
    target_row = i + 2
    
    print(f"🔎 [{i+1}] Processing: {raw_name}...", end=" ")
    
    # Get values via Multi-Market API
    vals = get_tradingview_data_multi_market(raw_name)
    
    if "N/A" in vals:
        print("❌ Failed")
    else:
        print(f"✅ Success: {vals[0]}")
    
    # Write to Sheet
    row_data = [raw_name, current_date] + vals
    try:
        dest_sheet.update(f"A{target_row}", [row_data])
    except Exception as e:
        print(f"❌ Write Error: {e}")

    # Faster sleep (API can handle it)
    time.sleep(0.2)

print("🏁 Done.")
