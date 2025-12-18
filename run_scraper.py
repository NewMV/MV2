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
START_INDEX = int(os.getenv("START_INDEX", "0")) # Start from 0 to capture all data_rows
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_shard_{SHARD_INDEX}.txt")

# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    # Check for credentials in environment variable first (for GitHub Actions)
    if os.getenv("GOOGLE_CREDENTIALS"):
        creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
        client = gspread.service_account_from_dict(creds_dict)
    else:
        client = gspread.service_account(filename="credentials.json")
        
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows    = source_sheet.get_all_values()[1:]  # Skip header
    print(f"✅ Shard {SHARD_INDEX} Connected. Total rows to check: {len(data_rows)}")
except Exception as e:
    print(f"❌ Connection Error: {e}")
    raise

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- DRIVER SETUP (REUSABLE) ---------------- #
def init_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--blink-settings=imagesEnabled=false") # Faster: No images
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    
    # Optional: Load cookies once at start
    if os.path.exists("cookies.json"):
        try:
            driver.get("https://www.tradingview.com/")
            with open("cookies.json", "r") as f:
                for c in json.load(f):
                    driver.add_cookie(c)
            driver.refresh()
        except: pass
    return driver

# ---------------- MAIN LOOP ---------------- #
last_i = START_INDEX
if os.path.exists(checkpoint_file):
    with open(checkpoint_file, "r") as f:
        try: last_i = int(f.read().strip())
        except: pass

driver = init_driver()

try:
    for i, row in enumerate(data_rows):
        target_row = i + 2 # Header is 1, Data starts at 2

        # Logic: Skip rows not belonging to this shard or range
        if i < last_i or i < START_INDEX or i > END_INDEX or i % SHARD_STEP != SHARD_INDEX:
            continue

        name = row[0]
        url  = row[3] if len(row) > 3 else ""

        print(f"🔎 Shard[{SHARD_INDEX}] Row {target_row}: {name}")

        vals = []
        if url:
            try:
                driver.get(url)
                WebDriverWait(driver, 20).until(
                    EC.visibility_of_element_located((By.XPATH, '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
                )
                soup = BeautifulSoup(driver.page_source, "html.parser")
                vals = [el.get_text().replace('−', '-').replace('∅', '').strip() 
                        for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")]
            except Exception as e:
                print(f"⚠️ Scrape Fail Row {target_row}: {e}")

        # Prepare and Upload
        row_data = [name, current_date] + (vals if vals else ["Error"] * 6)
        
        try:
            dest_sheet.update(f"A{target_row}", [row_data])
        except Exception as e:
            print(f"❌ Write Error Row {target_row}: {e}")
            time.sleep(5) # Cooldown for API limits

        # Update checkpoint
        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        # Respect Sheets API rate limits
        time.sleep(1)

finally:
    driver.quit()
    print(f"🏁 Shard {SHARD_INDEX} Finished.")
