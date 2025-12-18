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

# ---------------- CONFIG (Unchanged) ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = f"checkpoint_shard_{SHARD_INDEX}.txt"

# ---------------- AUTH (Updated for your Secret Names) ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    if creds_json:
        creds_dict = json.loads(creds_json)
        client = gspread.service_account_from_dict(creds_dict)
    else:
        client = gspread.service_account(filename="credentials.json")
        
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows    = source_sheet.get_all_values()[1:] 
    print(f"✅ Shard {SHARD_INDEX} Connected.")
except Exception as e:
    print(f"❌ Auth/Connection Error: {e}")
    raise

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- DRIVER SETUP (Reusable) ---------------- #
def init_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    
    # Load Cookies from your specific secret name
    cookies_json = os.getenv("TRADINGVIEW_COOKIES")
    if cookies_json:
        try:
            driver.get("https://www.tradingview.com/")
            for c in json.loads(cookies_json):
                driver.add_cookie(c)
            driver.refresh()
        except: pass
    return driver

# ---------------- MAIN LOOP (Main Logic Maintained) ---------------- #
last_i = START_INDEX
if os.path.exists(checkpoint_file):
    with open(checkpoint_file, "r") as f:
        try: last_i = int(f.read().strip())
        except: pass

driver = init_driver()

try:
    for i, row in enumerate(data_rows):
        target_row = i + 2 

        # MAIN LOGIC: Sharding and Range check
        if i < last_i or i < START_INDEX or i > END_INDEX or i % SHARD_STEP != SHARD_INDEX:
            continue

        name, url = row[0], row[3] if len(row) > 3 else ""
        print(f"🔎 Shard[{SHARD_INDEX}] Row {target_row}: {name}")

        vals = []
        if url:
            try:
                driver.get(url)
                # Matches your original WebDriverWait logic
                WebDriverWait(driver, 25).until(
                    EC.visibility_of_element_located((By.XPATH, '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
                )
                soup = BeautifulSoup(driver.page_source, "html.parser")
                vals = [el.get_text().replace('−', '-').replace('∅', '').strip() 
                        for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")]
            except: pass

        # Maintain exact order in output sheet
        row_data = [name, current_date] + (vals if vals else ["Error"] * 6)
        dest_sheet.update(f"A{target_row}", [row_data])
        
        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))
        
        time.sleep(1.2) # Protect against API rate limits

finally:
    driver.quit()
    print(f"🏁 Shard {SHARD_INDEX} Finished.")
