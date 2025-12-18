import os, time, json, gspread
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "1"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", "checkpoint_new_1.txt")

last_i = START_INDEX
if os.path.exists(checkpoint_file):
    with open(checkpoint_file, "r") as f:
        try: last_i = int(f.read().strip())
        except: pass

# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    client = gspread.service_account_from_dict(json.loads(creds_json)) if creds_json else gspread.service_account(filename="credentials.json")
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:]
    print("✅ Connected.")
except Exception as e:
    print(f"❌ Connection Error: {e}"); raise

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- OPTIMIZED SCRAPER ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def scrape_tradingview(driver, url):
    if not url: return []
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 15) # Reduced timeout for speed, handled by retry
        target_class = "valueValue-l31H9iuA"
        
        # WAIT until the elements have actual numbers (prevents "Error" / empty results)
        def data_is_ready(d):
            els = d.find_elements(By.CLASS_NAME, target_class)
            return any(any(c.isdigit() for c in e.text) for e in els[:3]) if els else False

        wait.until(data_is_ready)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        vals = [el.get_text().replace('−', '-').replace('∅', '').strip() 
                for el in soup.find_all("div", class_=target_class)]
        return vals if any(vals) else []
    except Exception as e:
        print(f"  ⚠️ Timeout/Fail for URL: {url[-20:]}")
        return []

# ---------------- MAIN LOOP ---------------- #
driver = get_driver()
batch, batch_start = [], None

try:
    for i, row in enumerate(data_rows):
        if i < last_i or i < START_INDEX or i > END_INDEX or i % SHARD_STEP != SHARD_INDEX:
            continue

        name, url = row[0], row[3] if len(row) > 3 else ""
        target_row = i + 2
        if batch_start is None: batch_start = target_row

        print(f"🔎 [{i}] {name}", end="\r")
        
        vals = scrape_tradingview(driver, url)
        
        # Fill with "Error" if scrape failed, but keep column structure
        row_data = [name, current_date] + (vals if vals else ["Error"] * 10)
        batch.append(row_data)

        # Batch update to Google Sheets (Every 5 stocks)
        if len(batch) >= 5:
            dest_sheet.update(f"A{batch_start}", batch)
            print(f"💾 Saved batch up to Row {target_row}         ")
            batch, batch_start = [], None
            time.sleep(1) # Small pause for Google API

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

    if batch: # Final flush
        dest_sheet.update(f"A{batch_start}", batch)

finally:
    driver.quit()
    print("\n🏁 Process finished.")
