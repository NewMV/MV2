import os
import time
import json
import gspread
import random
import re
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
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- 1. CONFIGURATION & SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_shard_{SHARD_INDEX}.txt")

# Load last processed index safely
last_i = START_INDEX
if os.path.exists(checkpoint_file):
    try:
        with open(checkpoint_file, "r") as f:
            content = f.read().strip()
            if content.isdigit():
                last_i = int(content)
    except Exception as e:
        print(f"⚠️ Checkpoint warning: {e}")

# ---------------- 2. GOOGLE SHEETS AUTHENTICATION ---------------- #
try:
    # Support for both file-based and environment-based credentials
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    if creds_json:
        gc = gspread.service_account_from_dict(json.loads(creds_json))
    else:
        gc = gspread.service_account("credentials.json")
    
    dest_sheet = gc.open("New MV2").worksheet("Sheet5")
    print("✅ Connected to Google Sheets: 'New MV2' -> 'Sheet5'")
except Exception as e:
    print(f"❌ Connection Error: {e}")
    exit(1)

# ---------------- 3. BROWSER SETUP (STABLE) ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# Using a single driver instance for maximum efficiency
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# ---------------- 4. DYNAMIC EXTRACTION LOGIC ---------------- #
def scrape_tradingview_values(url):
    if not url or not url.startswith("http"):
        return []
    try:
        driver.get(url)
        # Dynamic wait for TradingView indicator data
        DATA_CLASS = "valueValue-l31H9iuA"
        WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.CLASS_NAME, DATA_CLASS)))
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        # Capture ALL values in the container dynamiclly
        found_elements = soup.find_all("div", class_=re.compile(r"valueValue"))
        
        all_vals = []
        for el in found_elements:
            val_text = el.get_text(strip=True).replace('−', '-').replace('∅', '')
            if val_text and len(val_text) < 25: 
                all_vals.append(val_text)
        
        # Deduplicate while maintaining sequence
        return list(dict.fromkeys(all_vals))
    except Exception as e:
        print(f"  ⚠️ Scrape warning for {url}: {e}")
        return []

# ---------------- 5. EXECUTION LOOP ---------------- #
print(f"🚀 Shard {SHARD_INDEX} starting at index {last_i}")

try:
    # Fetch stock list from GitHub
    EXCEL_URL = "https://raw.githubusercontent.com/NewMV/MV2/main/Stock%20List%20.xlsx"
    response = requests.get(EXCEL_URL)
    response.raise_for_status()
    df = pd.read_excel(BytesIO(response.content), engine="openpyxl")
    name_list = df.iloc[:, 0].fillna("").tolist()
    url_list = df.iloc[:, 3].fillna("").tolist()
    print(f"✅ Loaded {len(url_list)} stocks.")
except Exception as e:
    print(f"❌ Excel Loading Error: {e}")
    driver.quit()
    exit(1)

curr_date = date.today().strftime("%m/%d/%Y")

for i in range(last_i, len(url_list)):
    if i > END_INDEX: break
    if i % SHARD_STEP != SHARD_INDEX: continue
    
    symbol = str(name_list[i])
    target_url = str(url_list[i])
    print(f"[{i}] Scraping: {symbol}")
    
    scraped_data = scrape_tradingview_values(target_url)
    
    # Structure: [Symbol, Date, Spacer] + All Found Values
    final_row = [symbol, curr_date, ""] + scraped_data
    
    try:
        dest_sheet.append_row(final_row)
        # Update checkpoint
        with open(checkpoint_file, "w") as f: 
            f.write(str(i))
    except Exception as e:
        print(f"  ❌ GSheet Error: {e}")
    
    # Randomized sleep to mimic human behavior
    time.sleep(3 + random.random() * 2)

driver.quit()
print("🎉 Process Finished.")
