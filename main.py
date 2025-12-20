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

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint.txt")

# Resume Logic
last_i = START_INDEX
if os.path.exists(CHECKPOINT_FILE):
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            last_i = int(f.read().strip())
    except: pass

# ---------------- GOOGLE SHEETS ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    client = gspread.service_account_from_dict(json.loads(creds_json)) if creds_json else gspread.service_account(filename="credentials.json")
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:]
except Exception as e:
    print(f"❌ Sheets Error: {e}"); raise

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- BROWSER SETUP (OPEN ONCE) ---------------- #
def init_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--blink-settings=imagesEnabled=false") # FAST: Disable Images
    opts.add_argument("--disable-gpu")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    
    # Cookie Injection (Once at startup)
    if os.path.exists("cookies.json"):
        driver.get("https://www.tradingview.com/")
        with open("cookies.json", "r") as f:
            for c in json.load(f):
                try: driver.add_cookie(c)
                except: pass
        driver.refresh()
    return driver

# ---------------- MAIN BATCH PROCESSING ---------------- #
driver = init_driver()
batch = []
batch_start = None

print(f"🚀 Starting Persistent Scraper (Rows {last_i+2} to {END_INDEX+2})")

try:
    for i, row in enumerate(data_rows):
        if i < last_i or i > END_INDEX:
            continue

        name = row[0]
        url = row[3] if len(row) > 3 else ""
        target_row = i + 2

        if batch_start is None: batch_start = target_row

        print(f"🔎 [{i}] {name}...", end=" ", flush=True)

        try:
            # FAST: Navigate without closing the browser
            driver.get(url)
            
            # Wait for specific data wrapper (Your XPath)
            WebDriverWait(driver, 15).until(
                EC.visibility_of_element_located((By.XPATH, '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
            )
            
            # Extract data
            soup = BeautifulSoup(driver.page_source, "html.parser")
            vals = [el.get_text().replace('−', '-').replace('∅', '').strip() 
                    for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")]
            
            # Ensure exactly 14 values (padding)
            while len(vals) < 14: vals.append("N/A")
            vals = vals[:14]

            row_data = [name, current_date] + vals
            batch.append(row_data)
            print("✅ Done")

        except Exception as e:
            print(f"⚠️ Fail: {str(e)[:30]}")
            batch.append([name, current_date] + ["Error"] * 14)

        # Batch Write Logic (Every 5 stocks)
        if len(batch) >= 5:
            try:
                dest_sheet.update(f"A{batch_start}", batch)
                print(f"💾 Saved rows {batch_start} to {target_row}")
                batch, batch_start = [], None
            except Exception as e:
                print(f"❌ Write Error: {e}")

        # Checkpoint
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(str(i + 1))

finally:
    if batch and batch_start:
        dest_sheet.update(f"A{batch_start}", batch)
    driver.quit()
    print("\n🏁 Process finished.")
