import os
import time
import json
import gspread
import requests
import pandas as pd
from datetime import date
from io import BytesIO
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- SHARDING (env-driven) ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "1")) 
END_INDEX = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", "checkpoint_new_1.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else START_INDEX

# ---------------- SETUP CHROME OPTIONS ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--window-size=1920,1080")

# ---------------- GOOGLE SHEETS AUTH & DATA FETCH ---------------- #
try:
    gc = gspread.service_account("credentials.json")
    sh = gc.open('New MV2')
    
    # READ FROM: "STOCK LIST"
    source_sheet = sh.worksheet('Stock List')
    # WRITE TO: "Sheet5"
    output_sheet = sh.worksheet('Sheet5')
    
    print("✅ Connected to Google Sheets.")

    # Fetch all data from the SOURCE sheet
    # Column A (Index 0) = Symbol, Column D (Index 3) = URL
    all_rows = source_sheet.get_all_values()
    
    # Skip Header (Row 1)
    data_rows = all_rows[1:] 
    name_list = [row[0] if len(row) > 0 else "" for row in data_rows]
    company_list = [row[3] if len(row) > 3 else "" for row in data_rows]
    
    print(f"✅ Loaded {len(company_list)} records from 'STOCK LIST'.")
except Exception as e:
    print(f"❌ Error accessing Google Sheets: {e}")
    exit(1)

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SCRAPER FUNCTION ---------------- #
def scrape_tradingview(company_url):
    if not company_url or not company_url.startswith("http"):
        return []

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )
    
    try:
        # Handle Cookies
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
        
        # Wait for data elements
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((By.XPATH, '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
        )
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        val_elements = soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        values = [el.get_text().replace('−', '-').replace('∅', '').strip() for el in val_elements]
        return values

    except Exception as e:
        print(f"❌ Scraping error for {company_url}: {e}")
        return []
    finally:
        driver.quit()

# ---------------- MAIN LOOP ---------------- #
for i, company_url in enumerate(company_list):
    
    current_idx = i + 1 
    
    if current_idx < last_i or current_idx < START_INDEX or current_idx > END_INDEX:
        continue
    if i % SHARD_STEP != SHARD_INDEX:
        continue

    name = name_list[i]
    # target_row matches the data index plus header offset
    target_row = i + 2 
    
    print(f"\n📌 Processing Index {current_idx} | {name} | Target Row: {target_row}")

    scraped_values = scrape_tradingview(company_url)

    if scraped_values:
        row_data = [name, current_date] + scraped_values
        try:
            # Updating Sheet5
            output_sheet.update(f'A{target_row}', [row_data])
            print(f"💾 Saved to 'Sheet5' Row {target_row} → {name}")
        except Exception as e:
            print(f"⚠️ Google Sheets update error: {e}")
    else:
        print(f"⚠️ No data captured for {name}")

    # Update Checkpoint
    with open(checkpoint_file, "w") as f:
        f.write(str(current_idx + 1))
    
    time.sleep(1)

print("\n✅ All tasks completed.")
