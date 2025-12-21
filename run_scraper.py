import os, time, json, gspread
from datetime import date
from tradingview_ta import TA_Handler, Interval

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
CHECKPOINT_FILE = "checkpoint.txt"

# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    if creds_json:
        client = gspread.service_account_from_dict(json.loads(creds_json))
    else:
        client = gspread.service_account(filename="credentials.json")
        
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:] 
    print("✅ Connected to Sheets")
except Exception as e:
    print(f"❌ Connection Error: {e}"); raise

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- WEBSOCKET DATA FETCH ---------------- #
def get_technical_data(symbol):
    """Explicitly fetches the 14 indicators via WebSocket protocol"""
    try:
        # Note: Change exchange="NASDAQ" and screener="america" for US stocks
        handler = TA_Handler(
            symbol=symbol,
            exchange="NSE",
            screener="india",
            interval=Interval.INTERVAL_1_DAY,
            timeout=None
        )
        analysis = handler.get_analysis()
        ind = analysis.indicators

        # Exactly mapping the 14 indicators requested
        return [
            str(ind.get("close")),        # 1. Price
            str(ind.get("change")),       # 2. Change Abs
            str(ind.get("RSI")),          # 3. RSI (14)
            str(ind.get("Stoch.K")),      # 4. Stochastic %K
            str(ind.get("CCI")),          # 5. CCI (20)
            str(ind.get("ADX")),          # 6. ADX (14)
            str(ind.get("AO")),           # 7. Awesome Oscillator
            str(ind.get("Mom")),          # 8. Momentum (10)
            str(ind.get("MACD.macd")),    # 9. MACD Level
            str(ind.get("Stoch.RSI.K")),  # 10. Stoch RSI
            str(ind.get("W.R")),          # 11. Williams %R
            str(ind.get("BBPower")),      # 12. Bull Bear Power
            str(ind.get("UO")),           # 13. Ultimate Oscillator
            str(analysis.summary.get("RECOMMENDATION")) # 14. Verdict
        ]
    except Exception as e:
        return ["N/A"] * 14

# ---------------- MAIN LOOP ---------------- #
last_i = START_INDEX
if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, "r") as f:
        try: last_i = int(f.read().strip())
        except: pass

batch = []
batch_start = None

print(f"🚀 Processing {START_INDEX} to {END_INDEX} via WebSocket")

for i, row in enumerate(data_rows):
    if i < last_i or i < START_INDEX or i > END_INDEX:
        continue

    symbol = row[0].strip()
    target_row = i + 2
    if batch_start is None: batch_start = target_row

    print(f"🔎 [{i}] {symbol} -> Row {target_row}")

    vals = get_technical_data(symbol)
    batch.append([symbol, current_date] + vals)

    # Batch Update (Fast: 10 symbols per update)
    if len(batch) >= 10:
        try:
            dest_sheet.update(f"A{batch_start}", batch)
            with open(CHECKPOINT_FILE, "w") as f: f.write(str(i + 1))
            batch, batch_start = [], None
            time.sleep(1.5) # Prevent Google API rate limit
        except Exception as e:
            print(f"❌ Write Error: {e}")

# Final Flush
if batch:
    dest_sheet.update(f"A{batch_start}", batch)

print("\n🏁 Process finished successfully.")
