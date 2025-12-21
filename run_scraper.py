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

# Resume from checkpoint
last_i = START_INDEX
if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, "r") as f:
        try:
            last_i = int(f.read().strip())
        except:
            pass

print(f"🔧 Range: {START_INDEX}-{END_INDEX} | Resume: {last_i}")

# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    if creds_json:
        client = gspread.service_account_from_dict(json.loads(creds_json))
    else:
        client = gspread.service_account(filename="credentials.json")
        
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:]  # Skip header
    print("✅ Connected. Reading Sheet1, Writing Sheet5")
except Exception as e:
    print(f"❌ Connection Error: {e}")
    raise

current_date = date.today().strftime("%m/%d/%Y")
CHROME_SERVICE = Service(ChromeDriverManager().install())

# ---------------- UPDATED SCRAPER (WITH DYNAMIC WAIT) ---------------- #
def scrape_tradingview(url):
    if not url:
        return []

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=CHROME_SERVICE, options=opts)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    try:
        # Cookies Logic
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
                    except: pass
            driver.refresh()

        driver.get(url)
        
        # --- NECESSARY CHANGE 1: DYNAMIC WAIT ---
        # Wait until the specific value element is not empty ("")
        wait = WebDriverWait(driver, 30)
        target_class = "valueValue-l31H9iuA"
        wait.until(lambda d: d.find_element(By.CLASS_NAME, target_class).text.strip() != "")

        # --- NECESSARY CHANGE 2: SCROLL TRIGGER ---
        # Forces TradingView to render the data in the divs
        driver.execute_script("window.scrollTo(0, 400);")
        time.sleep(1)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # --- NECESSARY CHANGE 3: NUMERIC FILTER ---
        extracted = []
        elements = soup.find_all("div", class_=target_class)
        for el in elements:
            val = el.get_text(strip=True).replace('−', '-').replace('∅', '').replace('+', '')
            # Only keep values that contain a digit (skip "Strong Buy", etc.)
            if val and any(char.isdigit() for char in val):
                if val not in extracted:
                    extracted.append(val)
        
        return extracted

    except Exception as e:
        print(f"⚠️ Scrape Fail: {str(e)[:50]}")
        return []

    finally:
        driver.quit()

# ---------------- YOUR PROVEN MAIN LOOP ---------------- #
batch, batch_start = [], None

print(f"\n🚀 Processing Rows {START_INDEX+2}-{END_INDEX+2}")

for i, row in enumerate(data_rows):
    if i < last_i or i < START_INDEX or i > END_INDEX:
        continue

    name = row[0]
    url  = row[3] if len(row) > 3 else ""
    target_row = i + 2

    if batch_start is None:
        batch_start = target_row

    print(f"🔎 [{i}] {name} -> Row {target_row}")

    vals = scrape_tradingview(url)
    
    # Ensure exactly 14 values are returned or filled with N/A
    final_vals = vals[:14]
    while len(final_vals) < 14:
        final_vals.append("N/A")
        
    row_data = [name, current_date] + final_vals
    batch.append(row_data)

    # Batch logic
    if len(batch) >= 5:
        try:
            dest_sheet.update(f"A{batch_start}", batch)
            print(f"💾 Saved rows {batch_start} to {target_row}")
            batch, batch_start = [], None
            time.sleep(2)
        except Exception as e:
            print(f"❌ Write Error: {e}")

    # Checkpoint
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(i + 1))

    time.sleep(1)

# Final Flush
if batch:
    dest_sheet.update(f"A{batch_start}", batch)

print("\n🏁 Process finished.")
