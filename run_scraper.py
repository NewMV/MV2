import os, time, json, gspread, re
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

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint.txt")

last_i = START_INDEX
if os.path.exists(CHECKPOINT_FILE):
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            last_i = int(f.read().strip())
    except: pass

# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    client = gspread.service_account_from_dict(json.loads(creds_json)) if creds_json else gspread.service_account(filename="credentials.json")
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:]
except Exception as e:
    print(f"❌ Connection Error: {e}"); raise

current_date = date.today().strftime("%m/%d/%Y")
CHROME_SERVICE = Service(ChromeDriverManager().install())

# ---------------- DRIVER SETUP ---------------- #
opts = Options()
opts.add_argument("--headless=new")
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-dev-shm-usage")
opts.add_argument("--disable-blink-features=AutomationControlled")
opts.add_experimental_option("excludeSwitches", ["enable-automation"])
opts.add_experimental_option("useAutomationExtension", False)
opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")

driver = webdriver.Chrome(service=CHROME_SERVICE, options=opts)
driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(driver, url, symbol_name):
    if not url: return ["No URL"] * 14
    try:
        driver.get(url)
        
        # We wait for the body first to ensure page loaded
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        
        # Try to find your exact XPath
        try:
            WebDriverWait(driver, 15).until(
                EC.visibility_of_element_located((By.XPATH, '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
            )
        except:
            print(f"  ⚠️ XPath not visible for {symbol_name}, attempting fallback scrape...")

        time.sleep(2) # Your required timing

        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # YOUR EXACT SELECTOR
        found_vals = []
        for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip"):
            val = el.get_text().replace('−', '-').replace('∅', '').strip()
            if val and val not in found_vals:
                found_vals.append(val)
        
        if not found_vals:
            return ["No Data Found"] * 14

        final_values = found_vals[:14]
        while len(final_values) < 14: final_values.append("N/A")
        return final_values

    except Exception as e:
        print(f"  ❌ Scrape error on {symbol_name}: {str(e)[:50]}")
        return ["Error"] * 14

# ---------------- MAIN LOOP ---------------- #
batch, batch_start = [], None

try:
    for i, row in enumerate(data_rows):
        if i < last_i or i < START_INDEX or i > END_INDEX: continue

        name, url, target_row = row[0], (row[3] if len(row) > 3 else ""), i + 2
        if batch_start is None: batch_start = target_row

        print(f"🔎 Processing: {name} (Row {target_row})")
        vals = scrape_tradingview(driver, url, name)
        
        batch.append([name, current_date] + vals)

        # UPDATED: Writes every 1 row so you can see progress immediately
        if len(batch) >= 1: 
            try:
                dest_sheet.update(f"A{target_row}", batch)
                print(f"✅ Saved: {name}")
                batch, batch_start = [], None
            except Exception as e:
                print(f"❌ Sheet Write Error: {e}")

        with open(CHECKPOINT_FILE, "w") as f: f.write(str(i + 1))
        time.sleep(1)

finally:
    driver.quit()
    print("🏁 Finished.")
