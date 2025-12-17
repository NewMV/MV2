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

# ---------------- SHARDING (main logic preserved) ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "1"))
END_INDEX = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", "checkpoint_new_1.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else START_INDEX

# ---------------- SETUP ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# ---------------- GOOGLE SHEETS AUTH & DYNAMIC READ ---------------- #
try:
    gc = gspread.service_account("credentials.json")
    sh = gc.open('New MV2')
    
    # Logic to find "Stock List" even if there are hidden spaces
    all_worksheets = sh.worksheets()
    source_sheet = None
    for sheet in all_worksheets:
        if "stock list" in sheet.title.lower().strip():
            source_sheet = sheet
            break
    
    if not source_sheet:
        available = [s.title for s in all_worksheets]
        print(f"❌ Error: Could not find 'Stock List'. Available tabs: {available}")
        exit(1)

    output_sheet = sh.worksheet('Sheet5')
    
    # Read data from Google Sheet instead of Github
    all_rows = source_sheet.get_all_values()
    data_rows = all_rows[1:]  # Skip header
    print(f"✅ Reading from source sheet: {source_sheet.title}")
except Exception as e:
    print(f"❌ Connection Error: {e}")
    exit(1)

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SCRAPER FUNCTION (main logic preserved) ---------------- #
def scrape_tradingview(company_url):
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )
    driver.set_window_size(1920, 1080)
    try:
        if os.path.exists("cookies.json"):
            driver.get("https://www.tradingview.com/")
            with open("cookies.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for cookie in cookies:
                try:
                    cookie_to_add = {k: cookie[k] for k in ('name', 'value', 'domain', 'path') if k in cookie}
                    driver.add_cookie(cookie_to_add)
                except: pass
            driver.refresh()
            time.sleep(2)

        driver.get(company_url)
        print(f"🔎 Visiting: {company_url}")
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((By.XPATH, '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [el.get_text().replace('−', '-').replace('∅', '').strip() for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")]
        return values
    except Exception as e:
        print(f"❌ Scraping error: {e}")
        return []
    finally:
        driver.quit()

# ---------------- MAIN LOOP (logic preserved + batching) ---------------- #
results_batch = []
batch_start_row = None 

for i, row in enumerate(data_rows):
    # Original sharding logic
    if i < last_i or i < START_INDEX or i > END_INDEX:
        continue
    if i % SHARD_STEP != SHARD_INDEX:
        continue

    # Main logic: Column A is Name, Column D (index 3) is URL
    name = row[0] if len(row) > 0 else f"Row {i}"
    company_url = row[3] if len(row) > 3 else ""
    target_row = i + 2 
    
    if batch_start_row is None:
        batch_start_row = target_row

    print(f"📌 Index {i} | {name} | Row: {target_row}")

    values = scrape_tradingview(company_url)

    # Sequence logic: Always keep name/date, fill Errors if scrape failed
    if values:
        row_data = [name, current_date] + values
    else:
        # Fills with Error but keeps Symbol/Date to maintain row order
        row_data = [name, current_date, "Error", "Error", "Error", "Error"]

    results_batch.append(row_data)

    # Compact request: Send 10 rows at once to stop the Write Error
    if len(results_batch) >= 10:
        try:
            output_sheet.update(f'A{batch_start_row}', results_batch)
            results_batch = []
            batch_start_row = None
            print(f"💾 Saved batch up to row {target_row}")
        except Exception as e:
            print(f"⚠️ Google Update Error: {e}")

    # Original Checkpoint logic
    with open(checkpoint_file, "w") as f:
        f.write(str(i + 1))
    
    time.sleep(1)

# Final upload for last rows
if results_batch:
    output_sheet.update(f'A{batch_start_row}', results_batch)

print("\n🏁 Scraping sequence completed.")
