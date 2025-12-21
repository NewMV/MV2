import os, time, json, gspread
from datetime import date
from tradingview_ta import TA_Handler, Interval, Exchange

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint.txt")

# ---------------- AUTH & CONNECT ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    client = gspread.service_account_from_dict(json.loads(creds_json)) if creds_json else gspread.service_account(filename="credentials.json")
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:]
except Exception as e:
    print(f"❌ Auth Error: {e}"); raise

current_date = date.today().strftime("%m/%d/%Y")

def get_tv_data(symbol):
    """Fetches 14 technical values via TradingView WebSocket Protocol"""
    try:
        # We assume NSE for Indian stocks, change Exchange.NASDAQ for US
        handler = TA_Handler(
            symbol=symbol,
            exchange="NSE", 
            screener="india",
            interval=Interval.INTERVAL_1_DAY
        )
        analysis = handler.get_analysis()
        
        # Mapping 14 specific technical indicators
        osc = analysis.indicators
        return [
            str(osc.get("close")),          # 1. Current Price
            str(osc.get("change")),         # 2. Change
            str(osc.get("RSI")),            # 3. RSI (14)
            str(osc.get("Stoch.K")),        # 4. Stochastic %K
            str(osc.get("CCI")),            # 5. CCI (20)
            str(osc.get("ADX")),            # 6. ADX (14)
            str(osc.get("AO")),             # 7. Awesome Oscillator
            str(osc.get("Mom")),            # 8. Momentum (10)
            str(osc.get("MACD.macd")),      # 9. MACD Level
            str(osc.get("Stoch.RSI.K")),    # 10. Stoch RSI
            str(osc.get("BBPower")),        # 11. Bull Bear Power
            str(osc.get("EMA10")),          # 12. EMA 10
            str(osc.get("SMA10")),          # 13. SMA 10
            analysis.summary.get("RECOMMENDATION") # 14. Overall Verdict
        ]
    except Exception as e:
        print(f"  ⚠️ Socket Fail for {symbol}: {e}")
        return ["N/A"] * 14

# ---------------- MAIN LOOP ---------------- #
last_i = START_INDEX
if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, "r") as f:
        try: last_i = int(f.read().strip())
        except: pass

batch = []
batch_start = None

print(f"🚀 WebSocket Processing: Rows {START_INDEX+2} to {END_INDEX+2}")

for i, row in enumerate(data_rows):
    if i < last_i or i < START_INDEX or i > END_INDEX: continue

    symbol = row[0].strip()
    if batch_start is None: batch_start = i + 2

    print(f"[{i}] Fetching {symbol}...", end="\r")
    
    vals = get_tv_data(symbol)
    batch.append([symbol, current_date] + vals)

    # Batch save every 10 (Faster since WebSocket is quick)
    if len(batch) >= 10:
        dest_sheet.update(f"A{batch_start}", batch)
        with open(CHECKPOINT_FILE, "w") as f: f.write(str(i + 1))
        batch, batch_start = [], None
        time.sleep(1) # Brief pause to respect Google Sheets rate limit

# Final Flush
if batch:
    dest_sheet.update(f"A{batch_start}", batch)

print("\n🏁 Process finished via WebSocket.")
