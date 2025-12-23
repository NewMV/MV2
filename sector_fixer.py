import os
import json
import time
import gspread
import random
from groq import Groq  # <-- Now using Groq
from datetime import date

# ---------------- CONFIGURATION ---------------- #
# We keep the name GEMINI_API_KEY as requested so you don't have to change your secrets
GROQ_API_KEY = os.getenv("GEMINI_API_KEY") 
SHEET_URL = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"
WORKSHEET_NAME = "Sheet9"

# Initialize Groq Client
client_ai = Groq(api_key=GROQ_API_KEY)

# Setup Google Sheets
def get_gspread_client():
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    return gspread.service_account_from_dict(json.loads(creds_json))

client = get_gspread_client()
sheet = client.open_by_url(SHEET_URL).worksheet(WORKSHEET_NAME)

def analyze_with_groq(symbol, sector_b, sector_c, max_retries=3):
    """Uses Groq to judge sectors and generate scope."""
    prompt = f"""
    Analyze the Indian company with Symbol: {symbol}.
    Potential sectors from my list: B: {sector_b}, C: {sector_c}.
    
    Tasks:
    1. Select the most accurate broad parent Sector (e.g., IT, Finance, Metals, Automotive, Mining, Energy). 
       Do NOT use sub-sectors like "Industrial Minerals".
    2. Write a 1-line future scope (max 12 words).

    Format output EXACTLY:
    SECTOR: [Sector Name]
    SCOPE: [Description]
    """
    
    for attempt in range(max_retries):
        try:
            # Using llama-3.3-70b for high reasoning quality
            chat_completion = client_ai.chat.completions.create(
                messages=[
                    {"role": "system", "content": "You are a factual financial analyst. Provide output only in the requested format."},
                    {"role": "user", "content": prompt}
                ],
                model="llama-3.3-70b-versatile",
                temperature=0.1, # Low temperature for consistency
            )
            
            res_text = chat_completion.choices[0].message.content
            sector, scope = "Manual Review", "No scope determined"
            
            for line in res_text.split('\n'):
                if "SECTOR:" in line.upper(): sector = line.split(":", 1)[1].strip()
                if "SCOPE:" in line.upper(): scope = line.split(":", 1)[1].strip()
                
            return sector, scope
            
        except Exception as e:
            wait_time = (2 ** attempt) + random.random()
            print(f"  ⏳ Groq Error for {symbol}: {e}. Retrying in {int(wait_time)}s...")
            time.sleep(wait_time)
            
    return "RETRY LATER", "Limit Reached"

# ---------------- MAIN EXECUTION ---------------- #
data = sheet.get_all_values()
rows = data[1:] 
updates = []

print(f"🚀 Groq AI Validation started for {len(rows)} rows...")

for i, row in enumerate(rows):
    symbol = row[0]
    s_b = row[1] if len(row) > 1 else ""
    s_c = row[2] if len(row) > 2 else ""
    
    # Skip if already processed to save tokens
    if len(row) > 3 and row[3].strip() != "":
        continue

    final_sector, future_scope = analyze_with_groq(symbol, s_b, s_c)
    row_idx = i + 2
    updates.append({'range': f'D{row_idx}:E{row_idx}', 'values': [[final_sector, future_scope]]})
    
    print(f"✅ {symbol} -> {final_sector}")
    
    # Groq is much faster, but we still respect limits (TPM/RPM)
    time.sleep(1) 
    
    if len(updates) >= 10:
        sheet.batch_update(updates)
        updates = []

if updates: sheet.batch_update(updates)
print("🏁 DONE")
