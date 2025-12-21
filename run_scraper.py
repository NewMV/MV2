import os, time, json, gspread, re
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint.txt")

# ---------------- G-SHEETS AUTH ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    client = gspread.service_account_from_dict(json.loads(creds_json)) if creds_json else gspread.service_account(filename="credentials.json")
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:]
except Exception as e:
    print(f"❌ Connection Error: {e}")
    raise

current_date = date.today().strftime("%m/%d/%Y")
CHROME_SERVICE = Service(ChromeDriverManager().install())

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=CHROME_SERVICE, options=opts)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def scrape_tradingview(url, symbol_name):
    if not url: return ["No URL"] * 14
    
    driver = get_driver()
    try:
        # 1. Load Cookies
        if os.path.exists("cookies.json"):
            driver.get("https://www.tradingview.com/")
            with open("cookies.json", "r") as f:
                for c in json.load(f):
                    try: driver.add_cookie(c)
                    except: pass
            driver.refresh()

        # 2. Strategic Loading
        driver.get(url)
        
        # Wait for the main data container to appear (Targeting the specific value class)
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "valueValue-l31H9iuA")))
        
        # Trigger Lazy Loading by scrolling
        driver.execute_script("window.scrollTo(0, 400);")
        time.sleep(1.5)
        driver.execute_script("window.scrollTo(0, 0);")
        
        # 3. Extraction
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Find all potential value containers
        raw_elements = soup.find_all("div", class_=re.compile(r"valueValue|value-"))
        
        extracted = []
        for el in raw_elements:
            val = el.get_text(strip=True).replace('−', '-').replace('∅', '')
            # Filter out UI labels or empty junk
            if val and len(val) < 20 and val not in extracted:
                extracted.append(val)
        
        # Fallback if the specific class failed
        if len(extracted) < 5:
            numeric_texts = soup.find_all(string=re.compile(r'^-?\d+'))
            for text in numeric_texts:
                clean = text.strip()
                if 0 < len(clean) < 15 and clean not in extracted:
                    extracted.append(clean)

        # Pad results
        final_values = extracted[:14]
        while len(final_values) < 14:
            final_values.append("N/A")
            
        return final_values

    except Exception as e:
        print(f"  ⚠️ Error on {symbol_name}: {str(e)[:50]}")
        return ["Error"] * 14
    finally:
        driver.quit()

# ---------------- MAIN LOOP ---------------- #
last_i = START_INDEX
if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, "r") as f:
        try: last_i = int(f.read().strip())
        except: pass

batch, batch_start = [], None

for i, row in enumerate(data_rows):
    if i < last_i or i < START_INDEX or i > END_INDEX: continue

    name = row[0]
    url = row[3] if len(row) > 3 else ""
    target_row = i + 2
    
    if batch_start is None: batch_start = target_row

    print(f"🔎 [{i}] {name[:15]}...", end=" ", flush=True)
    
    vals = scrape_tradingview(url, name)
    print(f"Found {len([v for v in vals if v != 'N/A'])} values.")
    
    batch.append([name, current_date] + vals)

    # Batch Update every 5 rows
    if len(batch) >= 5:
        try:
            dest_sheet.update(f"A{batch_start}", batch)
            with open(CHECKPOINT_FILE, "w") as f: f.write(str(i + 1))
            batch, batch_start = [], None
            time.sleep(1)
        except Exception as e:
            print(f"❌ Write Error: {e}")

# Final Flush
if batch:
    dest_sheet.update(f"A{batch_start}", batch)

print("\n🏁 Process finished.")
