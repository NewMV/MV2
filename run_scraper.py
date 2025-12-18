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

# CHANGE 1: default START_INDEX to 0 so first loop index is allowed
START_INDEX = int(os.getenv("START_INDEX", "0"))

END_INDEX   = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", "checkpoint_new_1.txt")

last_i = START_INDEX
if os.path.exists(checkpoint_file):
    with open(checkpoint_file, "r") as f:
        try:
            last_i = int(f.read().strip())
        except:
            pass

# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    # UPDATED: Support for GitHub Secret 'GSPREAD_CREDENTIALS'
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    if creds_json:
        client = gspread.service_account_from_dict(json.loads(creds_json))
    else:
        client = gspread.service_account(filename="credentials.json")
        
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")

    # get_all_values()[1:] skips header, so data_rows[0] is sheet row 2 [web:12][web:42]
    data_rows = source_sheet.get_all_values()[1:]
    print("✅ Connected. Reading Sheet1, Writing Sheet5")
except Exception as e:
    print(f"❌ Connection Error: {e}")
    raise

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SELENIUM SHARED SERVICE ---------------- #
CHROME_SERVICE = Service(ChromeDriverManager().install())

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(url):
    if not url:
        return []

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    
    # --- STEALTH ADDITIONS: Prevent Bot Detection ---
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=CHROME_SERVICE, options=opts)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    try:
        if os.path.exists("cookies.json"):
            driver.get("https://www.tradingview.com/")
            with open("cookies.json", "r") as f:
                for c in json.load(f):
                    try:
                        driver.add_cookie({
                            "name": c.get("name"),
                            "value": c.get("value"),
                            "domain": c.get("domain", ".tradingview.com"),
                            "path": c.get("path", "/")
                        })
                    except:
                        pass
            driver.refresh()

        driver.get(url)
        
        WebDriverWait(driver, 40).until(
            EC.visibility_of_element_located((
                By.XPATH,
                '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
            ))
        )
        
        time.sleep(2)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        return [
            el.get_text()
              .replace('−', '-')
              .replace('∅', '')
              .strip()
            for el in soup.find_all(
                "div",
                class_="valueValue-l31H9iuA apply-common-tooltip"
            )
        ]

    except Exception as e:
        print(f"⚠️ Scrape Fail: {e}")
        return []

    finally:
        driver.quit()

# ---------------- MAIN LOOP (Original Logic Maintained) ---------------- #
batch, batch_start = [], None

for i, row in enumerate(data_rows):  # i starts at 0 by default [web:5][web:41]
    if i < last_i or i < START_INDEX or i > END_INDEX or i % SHARD_STEP != SHARD_INDEX:
        continue

    name = row[0]
    url  = row[3] if len(row) > 3 else ""

    # CHANGE 2: map i=0 to sheet row 2 (row index + 2)
    target_row = i + 2

    if batch_start is None:
        batch_start = target_row

    print(f"🔎 [{i}] {name} -> Row {target_row}")

    vals = scrape_tradingview(url)
    row_data = [name, current_date] + (vals if vals else ["Error"] * 6)
    batch.append(row_data)

    if len(batch) >= 5:
        try:
            dest_sheet.update(f"A{batch_start}", batch)
            print(f"💾 Saved rows {batch_start} to {target_row}")
            batch, batch_start = [], None
            time.sleep(2)
        except Exception as e:
            print(f"❌ Write Error: {e}")

    with open(checkpoint_file, "w") as f:
        # CHANGE 3: checkpoint stores next index, so resume correctly from i
        f.write(str(i + 1))

    time.sleep(1)

# Final flush
if batch:
    dest_sheet.update(f"A{batch_start}", batch)

print("\n🏁 Process finished.")
