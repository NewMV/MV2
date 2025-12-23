import os
import json
import time
import gspread
import random
from groq import Groq
from datetime import date

# ---------------- CONFIGURATION ---------------- #
GRO_API_KEY = os.getenv("GEMINI_API_KEY") 
SHEET_URL = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"
WORKSHEET_NAME = "Sheet9"

client_ai = Groq(api_key=GRO_API_KEY)

def get_gspread_client():
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    return gspread.service_account_from_dict(json.loads(creds_json))

client = get_gspread_client()
sheet = client.open_by_url(SHEET_URL).worksheet(WORKSHEET_NAME)

def analyze_with_groq(symbol, sector_b, sector_c):
    prompt = f"Symbol: {symbol}. B: {sector_b}, C: {sector_c}. Task: Pick one broad Sector and 1-line future scope. Format: SECTOR: [Name] SCOPE: [Line]"
    
    # We use a while loop to keep trying until we get the data
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            chat_completion = client_ai.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model="llama-3.3-70b-versatile",
                temperature=0.1,
            )
            
            res_text = chat_completion.choices[0].message.content
            sector, scope = "Manual Review", "N/A"
            for line in res_text.split('\n'):
                if "SECTOR:" in line.upper(): sector = line.split(":", 1)[1].strip()
                if "SCOPE:" in line.upper(): scope = line.split(":", 1)[1].strip()
            return sector, scope

        except Exception as e:
            error_msg = str(e).lower()
            if "429" in error_msg or "rate_limit" in error_msg:
                # If we hit a limit, wait 65 seconds to fully reset the Groq minute-limit
                print(f"  ⏳ Rate Limit Hit for {symbol}. Cooling down for 65 seconds...")
                time.sleep(65)
            else:
                print(f"  ⚠️ Error for {symbol}: {e}")
                time.sleep(5)
                
    return "LIMIT_STILL_ACTIVE", "Check later"

# ---------------- MAIN EXECUTION ---------------- #
data = sheet.get_all_values()
rows = data[1:] 
updates = []

print(f"🚀 Processing with Rate-Limit Protection...")

for i, row in enumerate(rows):
    symbol = row[0]
    s_b = row[1] if len(row) > 1 else ""
    s_c = row[2] if len(row) > 2 else ""
    
    # CRITICAL: Skip rows that already have an AI Sector to save your limit
    if len(row) > 3 and row[3].strip() not in ["", "LIMIT_STILL_ACTIVE", "RETRY LATER"]:
        continue

    final_sector, future_scope = analyze_with_groq(symbol, s_b, s_c)
    row_idx = i + 2
    
    updates.append({'range': f'D{row_idx}:E{row_idx}', 'values': [[final_sector, future_scope]]})
    print(f"✅ [{i+1}] {symbol} -> {final_sector}")
    
    # Slow down to 1 request every 3 seconds (20 RPM) to stay under the 30 RPM limit
    time.sleep(3) 
    
    if len(updates) >= 5:
        try:
            sheet.batch_update(updates)
            updates = []
        except:
            time.sleep(10) # Wait if Google Sheets API is also complaining

if updates:
    sheet.batch_update(updates)

print("🏁 Finished.")
