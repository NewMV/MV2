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
    source_sheet = sh.worksheet('Stock List')
    output_sheet = sh.worksheet('Sheet5')
    
    # Batch Read: Get all values once to save read requests
    all_rows = source_sheet.get_all_values()
    data_rows = all_rows[1:] # Skip Header
    print(f"✅ Loaded {len(data_rows)} rows from 'STOCK LIST'.")
except Exception as e:
    print(f"❌ Auth/Read Error: {e}")
    exit(1)

current_date = date.today().strftime("%m/%d/%Y")

def scrape_tradingview(company_url):
    if not company_url or not company_url.startswith("http"):
        return []
    
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
        WebDriverWait(driver, 30).until(
            EC.visibility_of_element_located((By.CLASS_NAME, 'valueValue-l31H9iuA'))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        vals = [el.get_text().replace('−', '-').replace('∅', '').strip() for el in soup.find_all("div", class_="valueValue-l31H9iuA")]
        return vals
    except:
        return []
    finally:
        driver.quit()

# ---------------- MAIN PROCESSING ---------------- #
results_to_upload = []
last_processed_row = START_INDEX

# We process in the exact sequence of the source sheet
for i, row in enumerate(data_rows):
    current_row_num = i + 2 # Spreadsheet row index
    
    # Sharding Logic
    if current_row_num < START_INDEX or current_row_num > END_INDEX:
        continue
    if i % SHARD_STEP != SHARD_INDEX:
        continue

    symbol = row[0]
    url = row[3] if len(row) > 3 else ""

    print(f"🔎 [{current_row_num}] Scraping {symbol}...")
    scraped_data = scrape_tradingview(url)
    
    # Maintain sequence: [Symbol, Date, Val1, Val2...]
    row_output = [symbol, current_date] + scraped_data
    results_to_upload.append(row_output)
    
    # Update local checkpoint
    last_processed_row = current_row_num
    
    # Optional: Small batch write every 10 rows to prevent data loss if script crashes
    if len(results_to_upload) >= 10:
        start_range = f"A{last_processed_row - 9}"
        output_sheet.update(start_range, results_to_upload)
        print(f"💾 Batched 10 rows to Sheet5 (up to row {last_processed_row})")
        results_to_upload = [] # Clear batch
        with open(checkpoint_file, "w") as f: f.write(str(last_processed_row))
        time.sleep(2) # Pause to respect API limits

# Final upload for remaining rows
if results_to_upload:
    start_row = last_processed_row - len(results_to_upload) + 1
    output_sheet.update(f"A{start_row}", results_to_upload)
    with open(checkpoint_file, "w") as f: f.write(str(last_processed_row))

print("\n✅ Process Complete. Data synced in sequence.")
