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
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- YOUR EXACT CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

# YOUR ORIGINAL SHARD + BATCH LOGIC ✅
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "1"))
END_INDEX = int(os.getenv("END_INDEX", "2500"))
BATCH_INDEX = int(os.getenv("BATCH_INDEX", "0"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
COOKIE_PATH = "cookies.json"
MAX_RETRIES = 3

# ---------------- SHEETS ---------------- #
client = gspread.service_account(filename="credentials.json")
source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
dest_sheet = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")

all_rows = source_sheet.get_all_values()[1:]
chart_links = [row[3] if len(row) > 3 else "" for row in all_rows]

# YOUR ORIGINAL SHARD FILTERING ✅
account_links = [link for i, link in enumerate(chart_links) if 
                (i+1 >= START_INDEX and i+1 <= END_INDEX and (i+1) % SHARD_STEP == SHARD_INDEX)]

start = BATCH_INDEX * BATCH_SIZE
end = start + BATCH_SIZE
batch_links = account_links[start:end]

print(f"Shard {SHARD_INDEX}/{SHARD_STEP} | Batch {BATCH_INDEX} | URLs: {len(batch_links)}")

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- CHROME (YOUR FLAGS) ---------------- #
options = Options()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Cookies (YOUR WAY)
if os.path.exists(COOKIE_PATH):
    driver.get("https://www.tradingview.com/")
    with open(COOKIE_PATH, "r") as f:
        cookies = json.load(f)
        for cookie in cookies:
            cookie_data = {k: cookie[k] for k in ('name', 'value', 'domain', 'path') if k in cookie}
            try:
                driver.add_cookie(cookie_data)
            except: pass
    driver.refresh()

# ---------------- YOUR EXACT SCRAPER ✅ ---------------- #
def scrape_tradingview(url):
    if not url: return []
    try:
        driver.get(url)
        # YOUR ORIGINAL XPATH ✅
        WebDriverWait(driver, 35).until(EC.visibility_of_element_located((
            By.XPATH, 
            '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
        )))
        
        # YOUR ORIGINAL JS EXTRACTOR ✅
        values = driver.execute_script("""
            const studySections = document.querySelectorAll("[data-name='legend'] .item-l31H9iuA.study-l31H9iuA");
            const clubbed = Array.from(studySections).find(section => {
                const titleDiv = section.querySelector("[data-name='legend-source-title'] .title-l31H9iuA");
                const text = titleDiv?.innerText?.toLowerCase();
                return text === 'clubbed' || text === 'l';
            });
            if (!clubbed) return ['CLUBBED NOT FOUND'];
            const valueSpans = clubbed.querySelectorAll('.valueValue-l31H9iuA');
            const allValues = Array.from(valueSpans).map(el => {
                const text = el.innerText.trim();
                return text === '∅' ? 'None' : text.replace('−', '-');
            });
            return allValues.slice(1);
        """)
        return values or []
    except:
        return []

# ---------------- MAIN LOOP (SYMBOL→DATE→VALUES) ---------------- #
def safe_write(sheet, cell, data):
    for attempt in range(MAX_RETRIES):
        try:
            sheet.update(cell, data)
            return True
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
    return False

row_buffer = []
start_row = -1

for i, url in enumerate(batch_links):
    if not url: continue
    
    global_index = START_INDEX + (i * SHARD_STEP) + BATCH_INDEX * BATCH_SIZE
    symbol = all_rows[global_index-1][0] if global_index-1 < len(all_rows) else "Unknown"
    
    print(f"[{i+1}] {symbol}")
    values = scrape_tradingview(url)
    
    # **SYMBOL → DATE → VALUES** ✅
    row_data = [symbol, current_date] + (values + [""] * 8)[:8]
    row_buffer.append(row_data)
    
    if len(row_buffer) == 1:
        start_row = global_index + 1
    
    if len(row_buffer) >= BATCH_SIZE:
        if safe_write(dest_sheet, f'A{start_row}', row_buffer):
            print(f"💾 Batch saved: {len(row_buffer)} rows")
        row_buffer = []
        start_row = -1
        time.sleep(1)

if row_buffer:
    safe_write(dest_sheet, f'A{start_row}', row_buffer)

driver.quit()
print("🏁 DONE!")
