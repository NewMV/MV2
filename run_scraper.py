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

# ---------------- CONFIGURATION & SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_shard_{SHARD_INDEX}.txt")

# Load last processed index safely
last_i = START_INDEX
try:
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            content = f.read().strip()
            if content.isdigit():
                last_i = int(content)
except Exception as e:
    print(f"⚠️ Checkpoint read warning: {e}")

# ---------------- GOOGLE SHEETS SETUP ---------------- #
try:
    gc = gspread.service_account("credentials.json")
    dest_sheet = gc.open("New MV2").worksheet("Sheet5")
    print("✅ Connected to Google Sheets: 'New MV2' -> 'Sheet5'")
except Exception as e:
    print(f"❌ Connection Error: {e}")
    exit(1)

# ---------------- BROWSER SETUP (HEADLESS) ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# Single driver instance for speed
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Load cookies if available
if os.path.exists("cookies.json"):
    driver.get("https://www.tradingview.com/")
    try:
        with open("cookies.json", "r") as f:
            cookies = json.load(f)
        for cookie in cookies:
            try:
                if 'expiry' in cookie:
                    cookie['expiry'] = int(cookie['expiry'])
                driver.add_cookie(cookie)
            except: pass
        driver.refresh()
        time.sleep(2)
    except:
        print("⚠️ Cookie loading skipped")

# ---------------- EXTRACTION LOGIC ---------------- #
def scrape_all_container_values(url):
    if not url: return []
    try:
        driver.get(url)
        # Target the specific TradingView indicator value class
        DATA_CLASS = "valueValue-l31H9iuA"
        WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.CLASS_NAME, DATA_CLASS)))
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        # Find every element in the container without a hard limit
        found_elements = soup.find_all("div", class_=re.compile(r"valueValue"))
        
        all_values = []
        for el in found_elements:
            text = el.get_text(strip=True).replace('−', '-').replace('∅', '')
            # Filter for indicator values and price data
            if text and len(text) < 25: 
                all_values.append(text)
        
        # Deduplicate while preserving order
        return list(dict.fromkeys(all_values))
    except Exception as e:
        print(f"  ⚠️ Scrape Error: {e}")
        return []

# ---------------- MAIN BATCH PROCESSING ---------------- #
print(f"🚀 Starting Shard {SHARD_INDEX} from index {last_i} to {END_INDEX}")

try:
    EXCEL_URL = "https://raw.githubusercontent.com/NewMV/MV2/main/Stock%20List%20.xlsx"
    response = requests.get(EXCEL_URL)
    response.raise_for_status()
    df = pd.read_excel(BytesIO(response.content), engine="openpyxl")
    name_list = df.iloc[:, 0].fillna("").tolist()
    url_list = df.iloc[:, 3].fillna("").tolist()
    print(f"✅ Loaded {len(url_list)} stocks.")
except Exception as e:
    print(f"❌ FATAL: Could not load stock list: {e}")
    driver.quit()
    exit(1)

current_date = date.today().strftime("%m/%d/%Y")

# Iterate through the range based on SHARD settings
for i in range(last_i, len(url_list)):
    if i > END_INDEX: break
    if i % SHARD_STEP != SHARD_INDEX: continue
    
    name = str(name_list[i])
    url = str(url_list[i])
    print(f"[{i}] Processing: {name}")
    
    scraped_vals = scrape_all_container_values(url)
    
    # Structure: [Symbol, Date, Spacer] + All Scraped Values
    row_to_append = [name, current_date, ""] + scraped_vals
    
    try:
        # Append row individually to handle dynamic column counts
        dest_sheet.append_row(row_to_append)
        # Update progress
        with open(checkpoint_file, "w") as f: 
            f.write(str(i))
    except Exception as e:
        print(f"  ❌ GSheet Error: {e}")
    
    # Jittered sleep to avoid detection
    time.sleep(2 + random.random() * 2)

driver.quit()
print("🎉 Shard Complete.")
