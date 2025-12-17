from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
import gspread
from datetime import date
import os
import time
import json
import pandas as pd
import requests
from io import BytesIO
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- SHARDING (env-driven) ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "0")) # Set to 0 for standard lists
END_INDEX = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", "checkpoint_new_1.txt")

# Read last index from checkpoint
if os.path.exists(checkpoint_file):
    with open(checkpoint_file, "r") as f:
        last_i = int(f.read().strip())
else:
    last_i = START_INDEX

# ---------------- SETUP ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    gc = gspread.service_account("credentials.json")
    sheet_data = gc.open('New MV2').worksheet('Sheet5')
    print("✅ Connected to Google Sheet: Sheet5")
except Exception as e:
    print(f"❌ Error loading credentials.json: {e}")
    exit(1)

# ---------------- READ STOCK LIST FROM GITHUB ---------------- #
print("📥 Fetching stock list from GitHub Excel...")
try:
    EXCEL_URL = "https://raw.githubusercontent.com/NewMV/MV2/main/Stock%20List%20%20(3).xlsx"
    response = requests.get(EXCEL_URL)
    response.raise_for_status()

    df = pd.read_excel(BytesIO(response.content), engine="openpyxl")
    name_list = df.iloc[:, 0].fillna("").tolist()    # Column A
    company_list = df.iloc[:, 3].fillna("").tolist() # Column E
    print(f"✅ Loaded {len(company_list)} companies.")
except Exception as e:
    print(f"❌ Error reading Excel: {e}")
    exit(1)

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SCRAPER FUNCTION ---------------- #
def scrape_tradingview(company_url):
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )
    
    try:
        driver.get("https://www.tradingview.com/")
        if os.path.exists("cookies.json"):
            with open("cookies.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for cookie in cookies:
                try:
                    driver.add_cookie({k: cookie[k] for k in ('name', 'value', 'domain', 'path') if k in cookie})
                except: pass
            driver.refresh()
            time.sleep(2)

        driver.get(company_url)
        # Wait for the specific value container
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CLASS_NAME, "valueValue-l31H9iuA"))
        )

        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', '').strip()
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        return values
    except Exception as e:
        print(f"⚠️ Scraping error: {e}")
        return []
    finally:
        driver.quit()

# ---------------- MAIN LOOP (Row-Specific Writing) ---------------- #
# We iterate through the whole list, but only process indices that meet shard/checkpoint rules
for i in range(len(company_list)):

    # Skip logic
    if i < last_i or i < START_INDEX or i > END_INDEX:
        continue
    if i % SHARD_STEP != SHARD_INDEX:
        continue

    url = company_list[i]
    name = name_list[i]
    
    # CALCULATE TARGET ROW: 
    # If i=0 is your first company, it goes to Row 2 (Row 1 is Header)
    target_row = i + 2 

    print(f"\n📌 Processing Index {i} | {name} | Target Row: {target_row}")

    if not url or str(url).strip() == "":
        print(f"⏩ Skipping {name}: No URL found.")
        continue

    scraped_values = scrape_tradingview(url)

    if scraped_values:
        # Prepare the row: [Name, Date, Value1, Value2, ...]
        row_to_upload = [name, current_date] + scraped_values
        
        try:
            # Create a range (e.g., A2:Z2) to update only that specific row
            # This ensures index 5 always goes to row 7, maintaining perfect order
            end_col = chr(64 + len(row_to_upload)) if len(row_to_upload) <= 26 else "Z"
            cell_range = f'A{target_row}:{end_col}{target_row}'
            
            sheet_data.update(cell_range, [row_to_upload])
            print(f"💾 Row {target_row} updated successfully.")
        except Exception as e:
            print(f"❌ Sheets Update Error: {e}")
    else:
        print(f"⚠️ No data retrieved for {name}")

    # Update Checkpoint
    with open(checkpoint_file, "w") as f:
        f.write(str(i + 1))
    
    # Short rest to prevent API rate limits
    time.sleep(1)

print("\n✨ Scraper task completed.")
