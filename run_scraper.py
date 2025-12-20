import os, time, json, gspread, re
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
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
    except:
        pass

# ---------------- BROWSER SETUP (REUSABLE) ---------------- #
def create_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.page_load_strategy = 'eager'  # Speed boost: don't wait for images/ads
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

# ---------------- EXTRACTION LOGIC ---------------- #
def extract_data(driver, url, symbol_name):
    if not url: return [""] * 14
    try:
        driver.get(url)
        # Shorter wait for faster turnaround
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        
        all_values = []
        selectors = [".valueValue-l31H9iuA.apply-common-tooltip", "div[class*='valueValue']"]
        
        # Primary Selector Extraction
        for selector in selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for el in elements[:20]:
                text = el.text.strip().replace('−', '-').replace('∅', '')
                if text and len(text) < 25 and text not in all_values:
                    all_values.append(text)
        
        # Fallback Soup Extraction
        if len(all_values) < 5:
            soup = BeautifulSoup(driver.page_source, "html.parser")
            for div in soup.find_all('div', string=re.compile(r'[\d,.-]+'))[:15]:
                text = div.get_text().strip().replace('−', '-')
                if text not in all_values and len(text) < 25:
                    all_values.append(text)

        final_values = all_values[:14]
        while len(final_values) < 14: final_values.append("N/A")
        return final_values
    except Exception as e:
        print(f"  ⚠️ Error on {symbol_name}: {e}")
        return ["N/A"] * 14

# ---------------- MAIN ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    client = gspread.service_account_from_dict(json.loads(creds_json)) if creds_json else gspread.service_account(filename="credentials.json")
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:]
    current_date = date.today().strftime("%m/%d/%Y")
except Exception as e:
    print(f"❌ Connection Error: {e}"); raise

driver = create_driver()
batch, batch_start = [], None

try:
    for i, row in enumerate(data_rows):
        if i < last_i or i < START_INDEX or i > END_INDEX: continue
        
        name, url, target_row = row[0].strip(), (row[3] if len(row) > 3 else ""), i + 2
        if batch_start is None: batch_start = target_row

        print(f"🔎 [{i+1}/{END_INDEX}] {name}")
        vals = extract_data(driver, url, name)
        batch.append([name, current_date] + vals)

        if len(batch) >= 5:
            dest_sheet.update(f"A{batch_start}", batch)
            print(f"💾 Saved Rows {batch_start}-{target_row}")
            batch, batch_start = [], None
            
        with open(CHECKPOINT_FILE, "w") as f: f.write(str(i + 1))
        
        # Periodic restart to keep memory low (every 50 rows)
        if i % 50 == 0 and i != last_i:
            driver.quit()
            driver = create_driver()

finally:
    if batch: dest_sheet.update(f"A{batch_start}", batch)
    driver.quit()
    print("🏁 Done.")
