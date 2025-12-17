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

# ---------------- CONFIG (SAME AS JS) ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

# EXACT SAME BATCHING LOGIC AS JS
BATCH_INDEX = int(os.getenv("BATCH_INDEX", "0"))
ACCOUNT_START = int(os.getenv("ACCOUNT_START", "0"))
ACCOUNT_END = int(os.getenv("ACCOUNT_END", "2500"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
COOKIE_PATH = "cookies.json"

# ---------------- GOOGLE SHEETS (getChartLinks equivalent) ---------------- #
client = gspread.service_account(filename="credentials.json")
source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
dest_sheet = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")

# Read ALL data once (like JS getChartLinks)
all_rows = source_sheet.get_all_values()[1:]  # Skip header
chart_links = [row[3] if len(row) > 3 else "" for row in all_rows]

# EXACT SAME SLICING LOGIC AS JS
account_links = chart_links[ACCOUNT_START:ACCOUNT_END]
start = BATCH_INDEX * BATCH_SIZE
end = start + BATCH_SIZE
batch_links = account_links[start:end]

print(f"Account range: {ACCOUNT_START}–{ACCOUNT_END}")
print(f"Processing batch {BATCH_INDEX}: {start} to {end}")
print(f"Batch URLs: {len(batch_links)}")

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SINGLE BROWSER SETUP (CRITICAL) ---------------- #
options = Options()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-dev-tools")
options.add_argument("--disable-extensions")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
page = driver  # Single page reuse like JS

try:
    # Load cookies ONCE (like JS loadCookies)
    if os.path.exists(COOKIE_PATH):
        driver.get("https://www.tradingview.com/")
        with open(COOKIE_PATH, "r") as f:
            cookies = json.load(f)
            for cookie in cookies:
                cookie_data = {k: cookie[k] for k in ('name', 'value', 'domain', 'path') if k in cookie}
                try:
                    driver.add_cookie(cookie_data)
                except:
                    pass
        driver.refresh()
        print("✅ Cookies loaded")

    # ---------------- MAIN LOOP (EXACT JS LOGIC) ---------------- #
    row_buffer = []
    start_row = -1
    page_restart_count = 0

    for i, url in enumerate(batch_links):
        if not url:
            continue

        global_index = ACCOUNT_START + BATCH_INDEX * BATCH_SIZE + i
        print(f"Scraping Row {global_index + 2}: {url}")

        try:
            # safeGoto equivalent
            driver.get(url)
            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-name='legend']"))
            )

            # waitForFunction equivalent - wait for clubbed/L data
            WebDriverWait(driver, 15).until(
                """
                () => {
                    const sections = document.querySelectorAll("[data-name='legend'] .item-l31H9iuA.study-l31H9iuA");
                    for (const section of sections) {
                        const title = section.querySelector("[data-name='legend-source-title'] .title-l31H9iuA");
                        if (title?.innerText?.toLowerCase() === 'clubbed' || title?.innerText?.toLowerCase() === 'l') {
                            const values = section.querySelectorAll('.valueValue-l31H9iuA');
                            return Array.from(values).some(el => el.innerText.trim() && el.innerText.trim() !== '∅');
                        }
                    }
                    return false;
                }
                """
            )

            # $$eval equivalent - EXACT JS LOGIC
            values = driver.execute_script("""
                const studySections = document.querySelectorAll("[data-name='legend'] .item-l31H9iuA.study-l31H9iuA");
                const clubbed = Array.from(studySections).find(section => {
                    const titleDiv = section.querySelector("[data-name='legend-source-title"] .title-l31H9iuA');
                    const text = titleDiv?.innerText?.toLowerCase();
                    return text === 'clubbed' || text === 'l';
                });
                
                if (!clubbed) return ['CLUBBED NOT FOUND'];
                
                const valueSpans = clubbed.querySelectorAll('.valueValue-l31H9iuA');
                const allValues = Array.from(valueSpans).map(el => {
                    const text = el.innerText.trim();
                    return text === '∅' ? 'None' : text;
                });
                return allValues.slice(1);  // Skip first (title) like JS
            """)

            # Create row EXACTLY like JS: [date, ...values]
            row_data = [current_date] + values
            row_buffer.append(row_data)

            if len(row_buffer) == 1:
                start_row = global_index + 1  # Sheet row (1-based)

            # Write every 10 rows (like JS)
            if len(row_buffer) >= BATCH_SIZE:
                dest_sheet.update(f'A{start_row}', row_buffer)
                print(f"💾 Saved {len(row_buffer)} rows starting {start_row + 1}")
                row_buffer = []
                start_row = -1

            time.sleep(1)  # 1000ms delay like JS

        except Exception as e:
            print(f"❌ Error scraping {url}: {e}")
            row_buffer.append([current_date, "ERROR"])
            if len(row_buffer) == 1:
                start_row = global_index + 1

        # Restart page every 100 URLs (like JS)
        if (i + 1) % 100 == 0:
            print("🔄 Restarting page...")
            driver.quit()
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            page = driver
            if os.path.exists(COOKIE_PATH):
                driver.get("https://www.tradingview.com/")
                with open(COOKIE_PATH, "r") as f:
                    cookies = json.load(f)
                    for cookie in cookies:
                        cookie_data = {k: cookie[k] for k in ('name', 'value', 'domain', 'path') if k in cookie}
                        try:
                            driver.add_cookie(cookie_data)
                        except:
                            pass
                driver.refresh()
            page_restart_count += 1

    # Final batch (like JS)
    if row_buffer:
        if start_row == -1:
            start_row = global_index - len(row_buffer) + 2
        dest_sheet.update(f'A{start_row}', row_buffer)
        print(f"💾 Final batch: {len(row_buffer)} rows from row {start_row + 1}")

    print(f"🏁 Done! Page restarts: {page_restart_count}")

finally:
    driver.quit()
