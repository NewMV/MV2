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
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import tempfile

print("🚀 MV2 TradingView Scraper - GitHub Actions")

# CONFIG - EXACT JS ORDER
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

ACCOUNT_START = int(os.getenv("ACCOUNT_START", "0"))
ACCOUNT_END = int(os.getenv("ACCOUNT_END", "2500"))
BATCH_INDEX = int(os.getenv("BATCH_INDEX", "0"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))

print(f"📊 Range: {ACCOUNT_START}-{ACCOUNT_END} | Batch: {BATCH_INDEX}")

# GitHub Secrets → Temp Files
GSHEETS_CREDENTIALS = os.getenv("GSHEETS_CREDENTIALS")
COOKIES_JSON = os.getenv("COOKIES_JSON")

if not GSHEETS_CREDENTIALS:
    print("❌ GSHEETS_CREDENTIALS secret missing!")
    exit(1)

# Create temp files
with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
    f.write(GSHEETS_CREDENTIALS)
    creds_path = f.name

cookies_path = None
if COOKIES_JSON:
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        f.write(COOKIES_JSON)
        cookies_path = f.name

print("✅ Temp files created")

# SHEETS
client = gspread.service_account(filename=creds_path)
source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
dest_sheet = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
print("✅ Sheets connected")

all_rows = source_sheet.get_all_values()[1:]
chart_links = [row[3] if len(row) > 3 else "" for row in all_rows]
account_links = chart_links[ACCOUNT_START:ACCOUNT_END]
start = BATCH_INDEX * BATCH_SIZE
end = start + BATCH_SIZE
batch_links = account_links[start:end]

print(f"📈 {len(batch_links)} URLs to scrape")

current_date = date.today().strftime("%m/%d/%Y")

# CHROME - GitHub Actions Optimized
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-extensions")
options.add_argument("--no-zygote")
options.add_argument("--single-process")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
driver.set_page_load_timeout(45)

try:
    # Cookies
    if cookies_path:
        driver.get("https://www.tradingview.com/")
        time.sleep(3)
        try:
            with open(cookies_path) as f:
                cookies = json.load(f)
            for cookie in cookies[:15]:
                cookie_data = {
                    'name': cookie.get('name'),
                    'value': cookie.get('value'),
                    'domain': cookie.get('domain', '.tradingview.com'),
                    'path': cookie.get('path', '/')
                }
                driver.add_cookie(cookie_data)
            driver.refresh()
            time.sleep(3)
            print("✅ Cookies loaded")
        except:
            print("⚠️ No cookies")

    # MAIN LOOP - SYMBOL | DATE | VALUES
    row_buffer = []
    start_row = -1
    processed = 0

    for i, url in enumerate(batch_links):
        if not url.strip():
            continue

        global_index = ACCOUNT_START + i
        symbol = all_rows[ACCOUNT_START + i][0] if ACCOUNT_START + i < len(all_rows) else "UNKNOWN"

        print(f"[{processed+1}/{len(batch_links)}] {symbol}")

        try:
            driver.get(url)
            WebDriverWait(driver, 35).until(EC.visibility_of_element_located((
                By.XPATH, '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
            )))

            values = driver.execute_script("""
                const studySections = document.querySelectorAll("[data-name='legend'] .item-l31H9iuA.study-l31H9iuA");
                const clubbed = Array.from(studySections).find(section => {
                    const titleDiv = section.querySelector("[data-name='legend-source-title'] .title-l31H9iuA");
                    const text = titleDiv?.innerText?.toLowerCase();
                    return text === 'clubbed' || text === 'l';
                });
                if (!clubbed) return ['NO_CLUBBED'];
                const valueSpans = clubbed.querySelectorAll('.valueValue-l31H9iuA');
                return Array.from(valueSpans).slice(1).map(el => {
                    const text = el.innerText.trim();
                    return text === '∅' ? 'None' : text.replace('−', '-');
                });
            """) or ['NO_DATA']

            row_data = [symbol, current_date] + [str(v) for v in values[:8]]
            row_buffer.append(row_data)

            if len(row_buffer) == 1:
                start_row = global_index + 2

            processed += 1

        except Exception as e:
            print(f"  ❌ {e}")
            row_data = [symbol, current_date, "ERROR"] + [""] * 8
            row_buffer.append(row_data)
            if len(row_buffer) == 1:
                start_row = global_index + 2

        # Batch write every 50
        if len(row_buffer) >= BATCH_SIZE:
            try:
                dest_sheet.update(f'A{start_row}', row_buffer)
                print(f"💾 Saved {len(row_buffer)} rows at A{start_row}")
                row_buffer = []
                start_row = -1
                time.sleep(2)
            except Exception as e:
                print(f"❌ Write error: {e}")

    # Final batch
    if row_buffer:
        dest_sheet.update(f'A{start_row}', row_buffer)
        print(f"💾 Final batch saved")

    print(f"✅ COMPLETE! {processed} stocks scraped")

finally:
    driver.quit()
