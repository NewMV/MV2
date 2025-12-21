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
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG & SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_shard_{SHARD_INDEX}.txt")

last_i = START_INDEX
if os.path.exists(checkpoint_file):
    try:
        with open(checkpoint_file, "r") as f:
            content = f.read().strip()
            if content.isdigit(): last_i = int(content)
    except: pass

# ---------------- AUTH & BROWSER ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    gc = gspread.service_account_from_dict(json.loads(creds_json)) if creds_json else gspread.service_account("credentials.json")
    dest_sheet = gc.open("New MV2").worksheet("Sheet5")
except Exception as e:
    print(f"❌ Auth Error: {e}"); exit(1)

chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# Reuse one driver for the whole shard
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

def scrape_fast(url):
    if not url or not url.startswith("http"): return []
    try:
        driver.get(url)
        # Reduced timeout to 30s for speed; TradingView is usually faster than 60s
        DATA_CLASS = "valueValue-l31H9iuA"
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CLASS_NAME, DATA_CLASS)))
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        found_elements = soup.find_all("div", class_=re.compile(r"valueValue"))
        
        all_vals = []
        for el in found_elements:
            val_text = el.get_text(strip=True).replace('−', '-').replace('∅', '')
            if val_text and len(val_text) < 25: all_vals.append(val_text)
        
        return list(dict.fromkeys(all_vals))
    except: return []

# ---------------- EXECUTION ---------------- #
try:
    EXCEL_URL = "https://raw.githubusercontent.com/NewMV/MV2/main/Stock%20List%20.xlsx"
    df = pd.read_excel(BytesIO(requests.get(EXCEL_URL).content), engine="openpyxl")
    name_list, url_list = df.iloc[:, 0].tolist(), df.iloc[:, 3].tolist()
except Exception as e:
    print(f"❌ Excel Error: {e}"); driver.quit(); exit(1)

curr_date = date.today().strftime("%m/%d/%Y")
batch_data = []

for i in range(last_i, len(url_list)):
    if i > END_INDEX or i % SHARD_STEP != SHARD_INDEX: continue
    
    print(f"[{i}] Scraping: {name_list[i]}")
    scraped_data = scrape_fast(str(url_list[i]))
    batch_data.append([str(name_list[i]), curr_date, ""] + scraped_data)
    
    # Save to GSheet every 5 rows to balance speed vs safety
    if len(batch_data) >= 5:
        try:
            dest_sheet.append_rows(batch_data)
            batch_data = []
            with open(checkpoint_file, "w") as f: f.write(str(i))
        except: pass
    
    # Reduced sleep to 1-2 seconds
    time.sleep(1 + random.random())

# Final flush
if batch_data: dest_sheet.append_rows(batch_data)
driver.quit()
