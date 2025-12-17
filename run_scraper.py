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

# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    # Initialize gspread
    gc = gspread.service_account("credentials.json")
    
    # 1. Open Source: File named "Stock List", tab named "Sheet1"
    source_spreadsheet = gc.open('Stock List')
    source_sheet = source_spreadsheet.worksheet('Sheet1')
    
    # 2. Open Destination: File named "New MV2", tab named "Sheet5"
    dest_spreadsheet = gc.open('New MV2')
    output_sheet = dest_spreadsheet.worksheet('Sheet5')
    
    # Read source data
    all_rows = source_sheet.get_all_values()
    data_rows = all_rows[1:]  # Skip header
    print(f"✅ Auth Successful.")
    print(f"📋 Reading from: Stock List -> Sheet1")
    print(f"📝 Writing to: New MV2 -> Sheet5")

except Exception as e:
    print(f"❌ Connection Error: {str(e)}")
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
        # Preserving your exact XPATH logic
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
    # Original sharding logic preserved
    if i < last_i or i < START_INDEX or i > END_INDEX:
        continue
    if i % SHARD_STEP != SHARD_INDEX:
        continue

    # Input Logic: Symbol in Col A (0), Link in Col D (3)
    name = row[0] if len(row) > 0 else f"Row {i}"
    company_url = row[3] if len(row) > 3 else ""
    target_row = i + 2 
    
    if batch_start_row is None:
        batch_start_row = target_row

    print(f"📌 Index {i} | {name} | Row: {target_row}")
    values = scrape_tradingview(company_url)

    # Sequence logic: Ensure Symbol is always first; use "Error" if data missing
    if values:
        row_data = [name, current_date] + values
    else:
        # Maintaining alignment so your sheet sequence stays perfect
        # Adding 5 "Error" slots - adjust this to match your usual column count
        row_data = [name, current_date, "Error", "Error", "Error", "Error", "Error"]

    results_batch.append(row_data)

    # Batch writing: Send 5 rows at once (smaller batch is safer for quota)
    if len(results_batch) >= 5:
        try:
            output_sheet.update(f'A{batch_start_row}', results_batch)
            print(f"💾 Saved batch up to row {target_row}")
            results_batch = []
            batch_start_row = None
        except Exception as e:
            print(f"⚠️ Write Error: {e}")

    # Preserving your checkpoint system
    with open(checkpoint_file, "w") as f:
        f.write(str(i + 1))
    
    time.sleep(1)

# Final upload for remaining items
if results_batch and batch_start_row:
    output_sheet.update(f'A{batch_start_row}', results_batch)

print("\n🏁 Process finished.")
