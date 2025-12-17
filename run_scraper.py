import os
import time
import json
import gspread
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG & SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "1")) 
END_INDEX = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", "checkpoint_new_1.txt")

# ---------------- SETUP CHROME OPTIONS ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    gc = gspread.service_account("credentials.json")
    sh = gc.open('New MV2')
    
    # Target sheet names
    SOURCE_NAME = "Stock List"
    OUTPUT_NAME = "Sheet5"

    # Robust lookup to handle "Stock List" even with spaces or case issues
    all_titles = {sheet.title.strip(): sheet.title for sheet in sh.worksheets()}
    
    if SOURCE_NAME in all_titles:
        source_sheet = sh.worksheet(all_titles[SOURCE_NAME])
    else:
        # Fallback: check case-insensitive
        source_sheet = next((sh.worksheet(t) for s, t in all_titles.items() if s.lower() == SOURCE_NAME.lower()), None)

    if not source_sheet:
        print(f"❌ Error: Could not find '{SOURCE_NAME}'. Available: {list(all_titles.values())}")
        exit(1)

    output_sheet = sh.worksheet(OUTPUT_NAME)
    
    # Read everything at once to save API quota
    all_rows = source_sheet.get_all_values()
    data_rows = all_rows[1:]  # Skipping Header
    print(f"✅ Connected! Reading from '{source_sheet.title}' and writing to '{OUTPUT_NAME}'.")
    
except Exception as e:
    print(f"❌ Connection Error: {e}")
    exit(1)

current_date = date.today().strftime("%m/%d/%Y")

def scrape_tradingview(company_url):
    if not company_url or not company_url.startswith("http"):
        return None
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    try:
        if os.path.exists("cookies.json"):
            driver.get("https://www.tradingview.com/")
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            for c in cookies:
                try: driver.add_cookie({k: c[k] for k in ('name', 'value', 'domain', 'path') if k in c})
                except: pass
            driver.refresh()

        driver.get(company_url)
        # Wait for the specific data element
        WebDriverWait(driver, 30).until(
            EC.visibility_of_element_located((By.CLASS_NAME, 'valueValue-l31H9iuA'))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        vals = [el.get_text().replace('−', '-').replace('∅', '').strip() for el in soup.find_all("div", class_="valueValue-l31H9iuA")]
        return vals if vals else None
    except:
        return None
    finally:
        driver.quit()

# ---------------- MAIN PROCESSING ---------------- #
results_to_upload = []
# batch_start_row is where the first item in the current batch belongs in Sheet5
batch_start_row = START_INDEX + 1 

for i, row in enumerate(data_rows):
    current_row_num = i + 2 # The row position relative to the spreadsheet
    
    # Logic for Range and Sharding
    if current_row_num < (START_INDEX + 1) or current_row_num > (END_INDEX + 1):
        continue
    if i % SHARD_STEP != SHARD_INDEX:
        continue

    symbol = row[0] if len(row) > 0 else f"Unknown_{current_row_num}"
    url = row[3] if len(row) > 3 else ""

    print(f"🔎 Processing Row {current_row_num} | {symbol}...")
    scraped_data = scrape_tradingview(url)
    
    # ALWAYS keep the symbol name in the first column
    if scraped_data:
        row_output = [symbol, current_date] + scraped_data
    else:
        print(f"⚠️ Failed to scrape {symbol}. Filling with Error tags.")
        # Placeholders to maintain column alignment
        row_output = [symbol, current_date, "Error", "Error", "Error", "Error", "Error"]

    results_to_upload.append(row_output)
    
    # BATCH WRITE (Every 10 rows) to stay under Google API limits
    if len(results_to_upload) >= 10:
        output_sheet.update(f"A{batch_start_row}", results_to_upload)
        print(f"💾 Saved batch to Sheet5 (Rows {batch_start_row} to {current_row_num})")
        
        # Reset batch and move the starting pointer
        results_to_upload = []
        batch_start_row = current_row_num + 1
        
        # Update checkpoint file
        with open(checkpoint_file, "w") as f: f.write(str(current_row_num + 1))
        time.sleep(2) # Cooldown to avoid 429 Too Many Requests

# Final upload for remaining items
if results_to_upload:
    output_sheet.update(f"A{batch_start_row}", results_to_upload)
    with open(checkpoint_file, "w") as f: f.write(str(current_row_num + 1))

print("\n✅ Task completed. All data written in correct sequence.")
