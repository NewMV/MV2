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

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

BATCH_INDEX = int(os.getenv("BATCH_INDEX", "0"))
ACCOUNT_START = int(os.getenv("ACCOUNT_START", "0"))
ACCOUNT_END = int(os.getenv("ACCOUNT_END", "2500"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
COOKIE_PATH = "cookies.json"

# ---------------- SHEETS ---------------- #
client = gspread.service_account(filename="credentials.json")
source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
dest_sheet = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")

all_rows = source_sheet.get_all_values()[1:]
chart_links = [row[3] if len(row) > 3 else "" for row in all_rows]
account_links = chart_links[ACCOUNT_START:ACCOUNT_END]
start = BATCH_INDEX * BATCH_SIZE
end = start + BATCH_SIZE
batch_links = account_links[start:end]

print(f"Account range: {ACCOUNT_START}–{ACCOUNT_END}")
print(f"Processing batch {BATCH_INDEX}: {start} to {end}")
print(f"Batch URLs: {len(batch_links)}")

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- ULTRA-STABLE CHROME ✅ ---------------- #
def create_stable_driver():
    options = Options()
    
    # GitHub Actions PROVEN flags
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument("--disable-images")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
    
    # Memory stability
    options.add_argument("--max_old_space_size=4096")
    options.add_argument("--single-process")
    options.add_argument("--no-zygote")
    
    service = Service(ChromeDriverManager().install())
    service.log_path = '/tmp/chromedriver.log'  # Safe log path
    
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(45)
    driver.implicitly_wait = 10
    return driver

# ---------------- MAIN ---------------- #
driver = None
try:
    driver = create_stable_driver()
    print("✅ Driver created")
    
    # Test navigation
    print("🧪 Testing navigation...")
    driver.get("https://www.tradingview.com")
    time.sleep(3)
    print("✅ TradingView OK")
    
    # Safe cookies
    if os.path.exists(COOKIE_PATH):
        print("🔑 Loading cookies...")
        driver.get("https://www.tradingview.com")
        time.sleep(3)
        try:
            with open(COOKIE_PATH, "r") as f:
                cookies = json.load(f)
                for cookie in cookies[:20]:
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
                time.sleep(5)
                print("✅ Cookies loaded")
        except:
            print("⚠️ No cookies file")

    # ---------------- ROBUST SCRAPER ---------------- #
    def safe_scrape(url, symbol):
        try:
            print(f"  → Loading {symbol}...")
            driver.set_page_load_timeout(30)
            driver.get(url)
            time.sleep(5)  # Let chart render
            
            # Try clubbed section first
            values = driver.execute_script("""
                try {
                    const sections = document.query
