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
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
import tempfile
import sys

print("🚀 MV2 TradingView Scraper - PRODUCTION READY")

# CONFIG
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

ACCOUNT_START = int(os.getenv("ACCOUNT_START", "0"))
ACCOUNT_END = int(os.getenv("ACCOUNT_END", "2500"))
BATCH_INDEX = int(os.getenv("BATCH_INDEX", "0"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))

print(f"📊 Range: {ACCOUNT_START}-{ACCOUNT_END} | Batch: {BATCH_INDEX}")

# SECRETS
GSPREAD_CREDENTIALS = os.getenv("GSPREAD_CREDENTIALS")
TRADINGVIEW_COOKIES = os.getenv("TRADINGVIEW_COOKIES")

if not GSPREAD_CREDENTIALS:
    print("❌ GSPREAD_CREDENTIALS secret missing!")
    sys.exit(1)

# CREATE TEMP FILES
with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
    f.write(GSPREAD_CREDENTIALS)
    creds_path = f.name

cookies_path = None
if TRADINGVIEW_COOKIES:
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        f.write(TRADINGVIEW_COOKIES)
        cookies_path = f.name

print("✅ Temp files created")

# SHEETS SETUP
try:
    client = gspread.service_account(filename=creds_path)
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    print("✅ Sheets connected")
except Exception as e:
    print(f"❌ Sheets error: {e}")
    sys.exit(1)

# READ DATA
all_rows = source_sheet.get_all_values()[1:]
chart_links = [row[3] if len(row) > 3 else "" for row in all_rows]
account_links = chart_links[ACCOUNT_START:ACCOUNT_END]
start_idx = BATCH_INDEX * BATCH_SIZE
end_idx = min(start_idx + BATCH_SIZE, len(account_links))
batch_links = account_links[start_idx:end_idx]

print(f"📈 {len(batch_links)} URLs to process")

current_date = date.today().strftime("%m/%d/%Y")

# ULTRA-STABLE CHROME FOR GITHUB ACTIONS
def create_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument("--disable-images")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--single-process")
    options.add_argument("--no-zygote")
    
    service = Service(ChromeDriverManager().install())
    service.log_path = '/tmp/chromedriver.log'
    
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(40)
    driver.implicitly_wait = 10
    return driver

driver = None
try:
    driver = create_driver()
    print("✅ Chrome driver ready")

    # COOKIES (SAFE)
    if cookies_path and os.path.exists(cookies_path):
        try:
            driver.get("https://www.tradingview.com/")
            time.sleep(4)
            with open(cookies_path, "r") as f:
                cookies = json.load(f)
            for cookie in cookies[:10]:
                try:
                    cookie_data = {
                        'name': cookie.get('name'),
                        'value': cookie.get('value'),
                        'domain': cookie.get('domain', '.tradingview.com'),
                        'path': cookie.get('path', '/')
                    }
                    driver.add_cookie(cookie_data)
                except:
                    pass
            driver.refresh()
            time.sleep(4)
            print("✅ Cookies loaded")
        except Exception as e:
            print(f"⚠️ Cookies skipped: {e}")

    # MAIN SCRAPING LOOP
    row_buffer = []
    start_row = -1
    processed = 0
    failed = 0

    for i, url in enumerate(batch_links):
        if not url or not url.strip():
            continue

        global_index = ACCOUNT_START + start_idx + i
        symbol = all_rows[ACCOUNT_START + start_idx + i][0] if ACCOUNT_START + start_idx + i < len(all_rows) else f"UNKNOWN_{global_index}"

        print(f"[{processed+failed+1}/{len(batch_links)}] {symbol}")

        try:
            driver.get(url)
            time.sleep(4)

            # YOUR ORIGINAL XPATH
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((
                By.XPATH, '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
            )))

            # YOUR JS LOGIC
            values = driver.execute_script("""
                const studySections = document.querySelectorAll("[data-name='legend'] .item-l31H9iuA.study-l31H9iuA");
                const clubbed = Array.from(studySections).find(section => {
                    const titleDiv
