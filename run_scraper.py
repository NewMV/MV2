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
chrome_options.add_argument("--remote-debugging-port=9222")

# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    gc = gspread.service_account("credentials.json")
    sheet_data = gc.open('New MV2').worksheet('Sheet5')
    print("✅ Connected to Google Sheet: Sheet5")
except Exception as e:
    print(f"❌ Error loading credentials.json: {e}")
    exit(1)

# ---------------- READ STOCK LIST FROM GITHUB EXCEL ---------------- #
print("📥 Fetching stock list from GitHub Excel...")
try:
    EXCEL_URL = "https://raw.githubusercontent.com/NewMV/MV2/main/Stock%20List%20%20(3).xlsx"
    response = requests.get(EXCEL_URL)
    response.raise_for_status()
    df = pd.read_excel(BytesIO(response.content), engine="openpyxl")
    name_list = df.iloc[:, 0].fillna("").tolist()
    company_list = df.iloc[:, 3].fillna("").tolist()
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
    driver.set_window_size(1920, 1080)
    try:
        if os.path.exists("cookies.json"):
            driver.get("https://www.tradingview.com/")
            with open("cookies.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for cookie in cookies:
                try:
                    cookie_to_add = {k: cookie[k] for k in ('name', 'value', 'domain', 'path') if k in cookie}
                    cookie_to_add['secure'] = cookie.get('secure', False)
                    cookie_to_add['httpOnly'] = cookie.get('httpOnly', False)
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

# ---------------- MAIN LOOP ---------------- #
for i, company_url in enumerate(company_list):

    # Apply Sharding and Checkpoint logic
    if i < last_i or i < START_INDEX or i > END_INDEX:
        continue
    if i % SHARD_STEP != SHARD_INDEX:
        continue

    name = name_list[i] if i < len(name_list) else f"Row {i}"
    
    # Calculate target row (i=0 is index, so Row 2 if Row 1 is header)
    target_row = i + 2 
    print(f"\n📌 Index {i} | {name} | Target Row: {target_row}")

    values = scrape_tradingview(company_url)

    if values:
        row_data = [name, current_date] + values
        try:
            # We use update instead of append to maintain strict order
            # The range is defined as A{row} to Z{row} (or however many columns you have)
            sheet_data.update(f'A{target_row}', [row_data])
            print(f"💾 Saved to Sheet5 Row {target_row} → {name}")
        except Exception as e:
            print(f"⚠️ Google Sheets error for {name}: {e}")
    else:
        print(f"⚠️ No data scraped for {name}")

    # Update Checkpoint
    with open(checkpoint_file, "w") as f:
        f.write(str(i + 1)) # Point to the next index
        print(f"🧾 Checkpoint updated → {i + 1}")

    time.sleep(1)
