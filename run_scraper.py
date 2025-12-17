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

# ---------------- SHEETS SETUP ---------------- #
print("Connecting to Google Sheets...")
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

# ---------------- CHROME SETUP (CI-PROVEN) ---------------- #
def create_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
    options.add_argument("--disable-dev-tools")
    options.add_argument("--no-first-run")
    options.add_argument("--no-service-autorun")
    options.add_argument("--password-store=basic")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(45)
    driver.implicitly_wait = 10
    return driver

# ---------------- MAIN EXECUTION ---------------- #
driver = None
try:
    print("🚀 Starting Chrome driver...")
    driver = create_driver()
    print("✅ Driver ready")
    
    # Test connection
    print("🧪 Testing TradingView...")
    driver.get("https://www.tradingview.com")
    time.sleep(3)
    print("✅ TradingView accessible")
    
    # Load cookies safely
    if os.path.exists(COOKIE_PATH):
        print("🍪 Loading cookies...")
        driver.get("https://www.tradingview.com")
        time.sleep(2)
        try:
            with open(COOKIE_PATH, "r") as f:
                cookies = json.load(f)
            for cookie in cookies[:15]:  # Limit to prevent issues
                try:
                    cookie_data = {
                        'name': cookie.get('name'),
                        'value': cookie.get('value'),
                        'domain': cookie.get('domain', '.tradingview.com'),
                        'path': cookie.get('path', '/')
                    }
                    driver.add_cookie(cookie_data)
                except:
                    continue
            driver.refresh()
            time.sleep(4)
            print("✅ Cookies applied")
        except Exception as e:
            print(f"⚠️ Cookie load skipped: {e}")

    # ---------------- SCRAPER FUNCTION ---------------- #
    def scrape_page(url, symbol):
        try:
            print(f"  📊 Scraping {symbol}...")
            driver.get(url)
            time.sleep(6)  # Chart render time
            
            # Execute JavaScript - FIXED STRING (no triple quotes issue)
            js_script = """
const sections = document.querySelectorAll("[data-name='legend'] .item-l31H9iuA.study-l31H9iuA");
for (let section of sections) {
    const title = section.querySelector("[data-name='legend-source-title'] .title-l31H9iuA");
    if (title && (title.innerText.toLowerCase().includes('club') || title.innerText.toLowerCase() === 'l')) {
        const vals = section.querySelectorAll('.valueValue-l31H9iuA');
        const result = Array.from(vals).map(el => {
            let text = el.innerText.trim();
            return text === '\\u221e' || text === '∅' ? 'None' : text.replace('-', '-');
        }).slice(1);
        if (result.length > 0) return result;
    }
}
const fallback = document.querySelectorAll('.valueValue-l31H9iuA');
return Array.from(fallback).slice(0, 8).map(el => {
    let text = el.innerText.trim();
    return text === '\\u221e' || text === '∅' ? 'None' : text.replace('-', '-');
});
"""
            values = driver.execute_script(js_script)
            
            if not values:
                values = ['NO_DATA']
                
            return [str(v) for v in values[:8]]
            
        except Exception as e:
            print(f"  ❌ Error: {str(e)[:60]}")
            return ['ERROR']

    # ---------------- BATCH PROCESSING ---------------- #
    row_buffer = []
    start_row = -1
    
    for i, url in enumerate(batch_links):
        if not url or not url.strip():
            print(f"⏭️  Skipping empty URL {i}")
            continue
            
        global_index = ACCOUNT_START + BATCH_INDEX * BATCH_SIZE + i
        symbol = all_rows[ACCOUNT_START + i][0] if ACCOUNT_START + i < len(all_rows) else f"Row_{global_index}"
        print(f"Scraping Row {global_index + 2}: {symbol}")
        
        values = scrape_page(url, symbol)
        row_data = [current_date, symbol] + values
        
        row_buffer.append(row_data)
        
        if len(row_buffer) == 1:
            start_row = global_index + 2  # Sheet row (header + 1-based)
        
        # Write batch
        if len(row_buffer) >= BATCH_SIZE:
            try:
                dest_sheet.update(f'A{start_row}', row_buffer)
                print(f"💾 Saved {len(row_buffer)} rows → A{start_row}")
                row_buffer = []
                start_row = -1
                time.sleep(2)
            except Exception as e:
                print(f"❌ Sheet write error: {e}")
                time.sleep(1)

    # Final batch write
    if row_buffer and start_row != -1:
        try:
            dest_sheet.update(f'A{start_row}', row_buffer)
            print(f"💾 Final batch: {len(row_buffer)} rows → A{start_row}")
        except Exception as e:
            print(f"❌ Final write error: {e}")

    print("🎉 BATCH COMPLETE!")

except Exception as e:
    print(f"💥 Fatal error: {e}")

finally:
    if driver:
        try:
            driver.quit()
        except:
            pass
    print("🏁 Script finished")
