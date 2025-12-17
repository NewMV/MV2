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

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

BATCH_INDEX = int(os.getenv("BATCH_INDEX", "0"))
ACCOUNT_START = int(os.getenv("ACCOUNT_START", "0"))
ACCOUNT_END = int(os.getenv("ACCOUNT_END", "2500"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
COOKIE_PATH = "cookies.json"

# ---------------- SHEETS SETUP ---------------- #
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

# ---------------- CI/CD CHROME OPTIONS (CRASH-PROOF) ---------------- #
def create_driver():
    options = Options()
    
    # GitHub Actions / CI fixes
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-features=TranslateUI")
    options.add_argument("--disable-ipc-flooding-protection")
    
    # TradingView specific
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

driver = create_driver()

# ---------------- COOKIES ---------------- #
def load_cookies(driver):
    if os.path.exists(COOKIE_PATH):
        driver.get("https://www.tradingview.com/")
        time.sleep(2)
        try:
            with open(COOKIE_PATH, "r") as f:
                cookies = json.load(f)
                for cookie in cookies:
                    cookie_data = {k: cookie[k] for k in ('name', 'value', 'domain', 'path') if k in cookie}
                    try:
                        driver.add_cookie(cookie_data)
                    except:
                        pass
                driver.refresh()
                time.sleep(3)
                print("✅ Cookies loaded")
                return True
        except Exception as e:
            print(f"⚠️ Cookie error: {e}")
    return False

load_cookies(driver)

# ---------------- SCRAPER (SIMPLIFIED & ROBUST) ---------------- #
def scrape_chart(driver, url):
    try:
        print(f"  → Navigating...")
        driver.set_page_load_timeout(60)
        driver.get(url)
        
        # Wait for chart legend (more reliable)
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "[data-name='legend']"))
        )
        
        # Wait for clubbed/L section with data (JS logic)
        WebDriverWait(driver, 20).until(
            lambda d: d.execute_script("""
                const sections = document.querySelectorAll("[data-name='legend'] .item-l31H9iuA.study-l31H9iuA");
                for (const section of sections) {
                    const title = section.querySelector("[data-name='legend-source-title'] .title-l31H9iuA");
                    if (title && (title.innerText.toLowerCase() === 'clubbed' || title.innerText.toLowerCase() === 'l')) {
                        const values = section.querySelectorAll('.valueValue-l31H9iuA');
                        return Array.from(values).some(el => el.innerText.trim() && el.innerText.trim() !== '∅');
                    }
                }
                return false;
            """)
        )
        
        # Extract values (exact JS logic)
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
            return allValues.slice(1);  // Skip title
        """)
        
        return values if values else ["NO DATA"]
        
    except Exception as e:
        print(f"  ❌ Scrape error: {str(e)[:100]}")
        return ["ERROR"]

# ---------------- MAIN LOOP ---------------- #
row_buffer = []
start_row = -1

for i, url in enumerate(batch_links):
    if not url:
        continue
        
    global_index = ACCOUNT_START + BATCH_INDEX * BATCH_SIZE + i
    symbol = all_rows[ACCOUNT_START + i][0] if ACCOUNT_START + i < len(all_rows) else "Unknown"
    print(f"Scraping Row {global_index + 2}: {symbol}")
    
    values = scrape_chart(driver, url)
    row_data = [current_date] + values[:10]  # Limit columns
    
    row_buffer.append(row_data)
    
    if len(row_buffer) == 1:
        start_row = global_index + 2  # Sheet row (1-based + header)
    
    # Batch write
    if len(row_buffer) >= BATCH_SIZE:
        try:
            dest_sheet.update(f'A{start_row}', row_buffer)
            print(f"💾 Saved {len(row_buffer)} rows at {start_row}")
            row_buffer = []
            start_row = -1
        except Exception as e:
            print(f"❌ Write error: {e}")
        
        time.sleep(2)

# Final batch
if row_buffer:
    try:
        dest_sheet.update(f'A{start_row}', row_buffer)
        print(f"💾 Final batch: {len(row_buffer)} rows")
    except Exception as e:
        print(f"❌ Final write error: {e}")

driver.quit()
print("🏁 COMPLETE!")
