import os
import time
import json
import gspread
import random
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
import re

# ---------------- SHARDING (LIKE FIRST SCRIPT) ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = START_INDEX
if os.path.exists(checkpoint_file):
    try:
        last_i = int(open(checkpoint_file).read().strip())
    except:
        pass

print(f"🔧 Shard {SHARD_INDEX}/{SHARD_STEP} | Range {START_INDEX}-{END_INDEX} | Resume: {last_i}")

# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    if creds_json:
        client = gspread.service_account_from_dict(json.loads(creds_json))
    else:
        client = gspread.service_account("credentials.json")
    source_sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0").worksheet("Sheet1")
    dest_sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0").worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:]  # Skip header
    print("✅ Connected. Reading Sheet1, Writing Sheet5")
except Exception as e:
    print(f"❌ Connection Error: {e}")
    exit(1)

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SINGLE CHROME SETUP (OPTIMIZED) ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")

# SINGLE DRIVER (BIGGEST SPEEDUP!)
try:
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
except Exception as e:
    print(f"Error initializing WebDriver: {e}")
    exit(1)

# Load cookies ONCE (per shard)
if os.path.exists("cookies.json"):
    driver.get("https://www.tradingview.com/")
    with open("cookies.json", "r") as f:
        cookies = json.load(f)
    for cookie in cookies[:15]:  # Limit cookies
        try:
            cookie_to_add = {
                "name": cookie.get("name"),
                "value": cookie.get("value"),
                "domain": cookie.get("domain", ".tradingview.com"),
                "path": cookie.get("path", "/")
            }
            driver.add_cookie(cookie_to_add)
        except:
            pass
    driver.refresh()
    time.sleep(2)
else:
    print("⚠️ cookies.json not found")

# ---------------- CUSTOM WAIT (LIKE FIRST SCRIPT) ---------------- #
class text_content_loaded:
    def __init__(self, locator, min_count=10):
        self.locator = locator
        self.min_count = min_count

    def __call__(self, driver):
        elements = driver.find_elements(*self.locator)
        non_empty_count = sum(1 for el in elements if el.text.strip())
        return non_empty_count >= self.min_count

# ---------------- OPTIMIZED 14-VALUES SCRAPER ---------------- #
def scrape_tradingview(driver, url, symbol_name):
    if not url:
        return [""] * 14

    try:
        print(f"  🌐 {symbol_name[:20]}...")
        driver.set_page_load_timeout(60)
        driver.get(url)
        
        # Wait for data to fully render (75s like first script)
        DATA_LOCATOR = (By.CSS_SELECTOR, ".valueValue-l31H9iuA")
        WebDriverWait(driver, 75).until(text_content_loaded(DATA_LOCATOR, min_count=10))
        
        # YOUR PROVEN MULTI-STRATEGY EXTRACTION (EXACT!)
        all_values = []
        
        # Strategy 1: Primary selectors
        selectors = [
            ".valueValue-l31H9iuA.apply-common-tooltip",
            ".valueValue-l31H9iuA",
            "div[class*='valueValue']",
            "div[class*='value']",
            ".chart-markup-table .value",
            "[data-value]"
        ]
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for selector in selectors:
            elements = soup.select(selector)
            for el in elements[:20]:
                text = el.get_text().strip().replace('−', '-').replace('∅', '')
                if text and len(text) < 25:
                    all_values.append(text)
        
        # Strategy 2: Numeric divs
        numeric_divs = soup.find_all('div', string=re.compile(r'[\d,.-]+'))
        for div in numeric_divs[:15]:
            text = div.get_text().strip().replace('−', '-')
            if re.match(r'^[\d,.-]+.*|.*[\d,.-]+$', text) and len(text) < 25 and text not in all_values:
                all_values.append(text)
        
        # Strategy 3: Table cells
        tables = soup.find_all('table')
        for table in tables[:3]:
            for cell in table.find_all(['td', 'th'])[:20]:
                text = cell.get_text().strip().replace('−', '-')
                if re.match(r'[\d,.-]', text) and len(text) < 25 and text not in all_values:
                    all_values.append(text)
        
        # Dedupe + Pad to 14
        unique_values = list(dict.fromkeys(all_values))[:14]  # Preserve order
        final_values = unique_values + ["N/A"] * (14 - len(unique_values))
        
        print(f"  📊 {len(unique_values)} unique → {final_values[:3]}...")
        return final_values
        
    except (TimeoutException, NoSuchElementException):
        print(f"  ⏰ Timeout")
        return ["N/A"] * 14
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return ["N/A"] * 14

# ---------------- MAIN LOOP (SHARDED + BATCHED) ---------------- #
buffer = []
BATCH_SIZE = 50  # Bigger batches like first script
processed = success_count = 0

print(f"\n🚀 Processing {END_INDEX-START_INDEX+1} symbols → 16 columns")

for i, row in enumerate(data_rows):
    # SHARDING + RANGE CHECK
    if i % SHARD_STEP != SHARD_INDEX or i < last_i or i < START_INDEX or i > END_INDEX:
        continue
    
    if i > 2500:  # Hard limit
        break
        
    name = row[0].strip()
    url = row[3] if len(row) > 3 else ""
    target_row = i + 2
    
    print(f"[{i+1:4d}] {name[:25]} -> Row {target_row}")
    
    # Scrape with SINGLE driver
    vals = scrape_tradingview(driver, url, name)
    row_data = [name, current_date] + vals
    
    success = any(v != "N/A" for v in vals)
    if success:
        success_count += 1
    
    buffer.append(row_data)
    processed += 1
    
    # CHECKPOINT every row
    with open(checkpoint_file, "w") as f:
        f.write(str(i))
    
    # BATCH WRITE every 50 rows
    if len(buffer) >= BATCH_SIZE:
        try:
            batch_start = target_row - len(buffer) + 1
            dest_sheet.update(f"A{batch_start}", buffer)
            print(f"💾 Batch {len(buffer)} rows: {batch_start}-{target_row}")
            buffer.clear()
        except Exception as e:
            print(f"❌ Batch write failed: {e}")
    
    # Randomized sleep (1.5-3s like first script)
    time.sleep(1.5 + random.random() * 1.5)

# Final flush
if buffer:
    try:
        batch_start = target_row - len(buffer) + 1
        dest_sheet.update(f"A{batch_start}", buffer)
        print(f"💾 Final batch: {batch_start}-{target_row}")
    except Exception as e:
        print(f"❌ Final write failed: {e}")

driver.quit()
print(f"\n🎉 COMPLETE!")
print(f"📊 Processed: {processed} | Success: {success_count} | Rate: {success_count/processed*100:.1f}%")
print(f"📍 Sheet5: Rows {START_INDEX+2}-{END_INDEX+2} × 16 columns")
