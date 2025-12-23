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

# Setup Gemini AI
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-1.5-flash')

# Setup Google Sheets
def get_gspread_client():
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    if creds_json:
        # For GitHub Actions (using secrets)
        return gspread.service_account_from_dict(json.loads(creds_json))
    # For local testing
    return gspread.service_account(filename="credentials.json")

client = get_gspread_client()
sheet = client.open_by_url(SHEET_URL).worksheet(WORKSHEET_NAME)

def analyze_sector_with_ai(symbol, sector_b, sector_c):
    prompt = f"""
    Analyze the Indian company with Symbol: {symbol}.
    Potential sectors from my list: B: {sector_b}, C: {sector_c}.
    
    Tasks:
    1. Select the most accurate broad Sector (e.g., IT, Finance, Metals, Automotive, Mining, Energy). 
       Ignore sub-sectors like "Industrial Minerals" or "Software Services" - use only the parent category.
    2. Write a 1-line future scope (max 12 words).

    Format output EXACTLY:
    SECTOR: [Sector Name]
    SCOPE: [Description]
    """
    try:
        response = model.generate_content(prompt)
        lines = response.text.strip().split('\n')
        sector = lines[0].replace("SECTOR:", "").strip()
        scope = lines[1].replace("SCOPE:", "").strip()
        return sector, scope
    except:
        return "Manual Review", "AI could not determine scope"

# ---------------- MAIN ---------------- #
data = sheet.get_all_values()
rows = data[1:] # Skip headers
updates = []

print(f"🚀 AI Validation started for {len(rows)} rows...")

for i, row in enumerate(rows):
    symbol = row[0]
    s_b = row[1] if len(row) > 1 else ""
    s_c = row[2] if len(row) > 2 else ""
    
    # AI Decision Logic
    final_sector, future_scope = analyze_sector_with_ai(symbol, s_b, s_c)
    
    # Mapping back to Columns D and E
    row_idx = i + 2
    updates.append({
        'range': f'D{row_idx}:E{row_idx}',
        'values': [[final_sector, future_scope]]
    })
    
    print(f"✅ {symbol} -> {final_sector}")
    
    # Throttle for API rate limits
    time.sleep(1)
    
    # Save every 10 rows to prevent data loss
    if len(updates) >= 10:
        sheet.batch_update(updates)
        updates = []

# Final Save
if updates:
    sheet.batch_update(updates)

print("🏁 Process Complete.")
