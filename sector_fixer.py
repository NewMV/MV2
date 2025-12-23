import os
import json
import time
import gspread
import google.generativeai as genai
from datetime import date

# ---------------- CONFIGURATION ---------------- #
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
SHEET_URL = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"
WORKSHEET_NAME = "Sheet9"

# 1. Setup Gemini with safety overrides to prevent "AI could not determine" errors
genai.configure(api_key=GEMINI_API_KEY)
# Turning off filters as much as possible for factual stock data
safety_settings = [
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
]
model = genai.GenerativeModel('gemini-1.5-flash', safety_settings=safety_settings)

# Setup Google Sheets
def get_gspread_client():
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    return gspread.service_account_from_dict(json.loads(creds_json))

client = get_gspread_client()
sheet = client.open_by_url(SHEET_URL).worksheet(WORKSHEET_NAME)

def analyze_sector_with_ai(symbol, sector_b, sector_c):
    prompt = f"""
    Symbol: {symbol}. 
    Data B: {sector_b}, C: {sector_c}.
    Task: Pick the broad parent sector (IT, Finance, Metals, etc.) and write a 1-line future scope.
    Format your response EXACTLY like this:
    SECTOR: [Sector Name]
    SCOPE: [Description]
    """
    try:
        response = model.generate_content(prompt)
        full_text = response.text
        
        # 2. SMART PARSING (Finds labels even if AI adds extra words)
        sector = "Unknown"
        scope = "No scope found"
        
        for line in full_text.split('\n'):
            if "SECTOR:" in line.upper():
                sector = line.split(":", 1)[1].strip()
            if "SCOPE:" in line.upper():
                scope = line.split(":", 1)[1].strip()
                
        return sector, scope
    except Exception as e:
        print(f"  ❌ AI Error for {symbol}: {e}")
        return "RETRY NEEDED", "Blocked or API Limit reached"

# ---------------- MAIN ---------------- #
data = sheet.get_all_values()
rows = data[1:] 
updates = []

for i, row in enumerate(rows):
    symbol = row[0]
    s_b = row[1] if len(row) > 1 else ""
    s_c = row[2] if len(row) > 2 else ""
    
    final_sector, future_scope = analyze_sector_with_ai(symbol, s_b, s_c)
    row_idx = i + 2
    updates.append({
        'range': f'D{row_idx}:E{row_idx}',
        'values': [[final_sector, future_scope]]
    })
    
    print(f"✅ {symbol} -> {final_sector}")
    
    # 3. RATE LIMIT PROTECTION: Sleep 3-4 seconds per request for Free Tier
    time.sleep(4)
    
    if len(updates) >= 5:
        sheet.batch_update(updates)
        updates = []

if updates: sheet.batch_update(updates)
print("🏁 Done!")
