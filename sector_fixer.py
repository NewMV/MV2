import os
import json
import time
import gspread
import random
import re
from groq import Groq

# ---------------- CONFIG ---------------- #
GROQ_API_KEY = os.getenv("GEMINI_API_KEY") 
SHEET_URL = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"
WORKSHEET_NAME = "Sheet9"

client_ai = Groq(api_key=GROQ_API_KEY)

def get_gspread_client():
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    return gspread.service_account_from_dict(json.loads(creds_json))

client = get_gspread_client()
sheet = client.open_by_url(SHEET_URL).worksheet(WORKSHEET_NAME)

def analyze_with_groq(symbol, sector_b, sector_c):
    # SHORTER PROMPT = FEWER TOKENS = NO LIMIT ERRORS
    prompt = f"Symbol:{symbol}. B:{sector_b}, C:{sector_c}. Task: 1.Broad Sector 2.Future scope (12 words). Format: SECTOR: [Name] SCOPE: [Line]"
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            chat_completion = client_ai.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model="llama-3.3-70b-versatile",
                temperature=0.1,
            )
            
            res_text = chat_completion.choices[0].message.content
            sector, scope = "Manual Check", "N/A"
            for line in res_text.split('\n'):
                if "SECTOR:" in line.upper(): sector = line.split(":", 1)[1].strip()
                if "SCOPE:" in line.upper(): scope = line.split(":", 1)[1].strip()
            return sector, scope

        except Exception as e:
            error_str = str(e)
            # Check for Rate Limit (429)
            if "429" in error_str or "rate_limit" in error_str:
                # Look for Groq's suggested wait time in the error message
                wait_match = re.search(r"retry after ([\d.]+s|[\d.]+ms)", error_str)
                wait_time = 70 # Default to 70s if we can't find the exact time
                if wait_match:
                    wait_str = wait_match.group(1)
                    wait_time = float(wait_str.replace('s', '')) + 2 if 's' in wait_str else 2
                
                print(f"  ⏳ Limit reached for {symbol}. Waiting {wait_time}s to reset tokens...")
                time.sleep(wait_time)
            else:
                print(f"  ⚠️ Error: {e}")
                time.sleep(10)
                
    return "RETRY_LATER", "Limit hit"

# ---------------- MAIN ---------------- #
data = sheet.get_all_values()
rows = data[1:] 

print(f"🚀 AI Validation with Token-Saving Logic...")

for i, row in enumerate(rows):
    symbol = row[0]
    s_b = row[1] if len(row) > 1 else ""
    s_c = row[2] if len(row) > 2 else ""
    
    # Skip already done rows
    if len(row) > 3 and row[3].strip() not in ["", "LIMIT_STILL_ACTIVE", "RETRY_LATER", "Check later"]:
        continue

    final_sector, future_scope = analyze_with_groq(symbol, s_b, s_c)
    row_idx = i + 2
    
    # Write INSTANTLY to ensure no data is lost if GitHub crashes
    try:
        sheet.update(f"D{row_idx}:E{row_idx}", [[final_sector, future_scope]])
        print(f"✅ [{i+1}] {symbol} saved.")
    except Exception as e:
        print(f"  ❌ GSheet update failed: {e}")
        time.sleep(5)
    
    # Wait 6 seconds between stocks. 10 stocks/minute = safe for free tier TPM.
    time.sleep(6) 

print("🏁 DONE")
