import os, time, json, gspread, requests
from datetime import date

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint.txt")

# ---------------- GOOGLE SHEETS ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    client = gspread.service_account_from_dict(json.loads(creds_json)) if creds_json else gspread.service_account(filename="credentials.json")
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:]
except Exception as e:
    print(f"❌ Connection Error: {e}"); raise

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- FAST API SCRAPER ---------------- #
def get_tradingview_data(symbol):
    """Fetches technical data directly via POST request"""
    # TradingView expects symbols in 'EXCHANGE:SYMBOL' format (e.g., NASDAQ:AAPL)
    # If your sheet only has 'AAPL', we try to guess or use a default
    formatted_symbol = symbol if ":" in symbol else f"NASDAQ:{symbol}"
    
    url = "https://scanner.tradingview.com/global/scan"
    
    payload = {
        "symbols": {"tickers": [formatted_symbol.upper()]},
        "columns": [
            "Recommend.All", "RSI", "Stoch.K", "Stoch.D", 
            "EMA10", "SMA10", "EMA20", "SMA20", 
            "EMA30", "SMA30", "EMA50", "SMA50", 
            "EMA100", "SMA100"
        ]
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        data = response.json()
        if "data" in data and len(data["data"]) > 0:
            # Extract the 'd' list which contains the column values
            values = data["data"][0]["d"]
            # Format numbers to 2 decimal places and handle None
            return [str(round(v, 2)) if isinstance(v, (int, float)) else "N/A" for v in values]
        return ["N/A"] * 14
    except Exception as e:
        print(f"  ⚠️ API Error for {symbol}: {e}")
        return ["Error"] * 14

# ---------------- MAIN LOOP ---------------- #
print(f"🚀 Starting fast API scrape...")

for i, row in enumerate(data_rows):
    name = row[0].strip()
    target_row = i + 2
    
    # Simple check: skip if we already have a checkpoint (Optional)
    # if i < last_i: continue

    print(f"🔎 [{i+1}] Fetching {name}...")
    
    # Get values via API (Instant)
    vals = get_tradingview_data(name)
    
    # Write to Sheet5 immediately
    row_data = [name, current_date] + vals
    try:
        dest_sheet.update(f"A{target_row}", [row_data])
        print(f"✅ Saved {name} to Row {target_row}")
    except Exception as e:
        print(f"❌ Write Error: {e}")

    # Small sleep just to be polite to the API
    time.sleep(0.5)

print("🏁 Process finished.")
