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
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "1"))
END_INDEX = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", "checkpoint_new_1.txt")

last_i = START_INDEX
if os.path.exists(checkpoint_file):
    with open(checkpoint_file, "r") as f:
        try: last_i = int(f.read().strip())
        except: pass

# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    client = gspread.service_account(filename="credentials.json")
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:]  # Skip header
    print(f"✅ Connected. Reading Sheet1, Writing Sheet5")
except Exception as e:
    print(f"❌ Connection Error: {e}"); raise

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(url):
    if not url: return []
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    try:
        if os.path.exists("cookies.json"):
            driver.get("https://www.tradingview.com/")
            with open("cookies.json", "r") as f:
                for c in json.load(f):
                    try: driver.add_cookie({k: c[k] for k in ('name', 'value', 'domain', 'path') if k in c})
                    except: pass
            driver.refresh()

        driver.get(url)
        WebDriverWait(driver, 35).until(EC.visibility_of_element_located((By.XPATH, '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div')))
        soup = BeautifulSoup(driver.page_source, "html.parser")
        return [el.get_text().replace('−', '-').replace('∅', '').strip() for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")]
    except Exception as e:
        print(f"⚠️ Scrape Fail: {e}"); return []
    finally: driver.quit()

# ---------------- MAIN LOOP ---------------- #
batch, batch_start = [], None

for i, row in enumerate(data_rows):
    if i < last_i or i < START_INDEX or i > END_INDEX or i % SHARD_STEP != SHARD_INDEX:
        continue

    name, url = row[0], row[3] if len(row) > 3 else ""
    target_row = i + 2
    if batch_start is None: batch_start = target_row

    print(f"🔎 [{i}] {name} -> Row {target_row}")
    vals = scrape_tradingview(url)
    
    # Maintain Column order: Symbol, Date, then Scraped Data or Errors
    row_data = [name, current_date] + (vals if vals else ["Error"] * 6)
    batch.append(row_data)

    # Write to Sheet in batches of 5 to stay under API limits
    if len(batch) >= 5:
        try:
            dest_sheet.update(f'A{batch_start}', batch)
            print(f"💾 Saved rows {batch_start} to {target_row}")
            batch, batch_start = [], None
        except Exception as e: print(f"❌ Write Error: {e}")

    with open(checkpoint_file, "w") as f: f.write(str(i + 1))
    time.sleep(1)

# Final cleanup for remaining rows
if batch:
    dest_sheet.update(f'A{batch_start}', batch)

print("\n🏁 Process finished.")
