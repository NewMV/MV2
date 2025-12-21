import os, time, json, gspread
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
import re

# ---------------- CONFIG (YOUR EXACT) ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint.txt")

# Resume from checkpoint (YOUR EXACT)
last_i = START_INDEX
if os.path.exists(CHECKPOINT_FILE):
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            last_i = int(f.read().strip())
    except:
        pass

print(f"🔧 Range: {START_INDEX}-{END_INDEX} | Resume: {last_i}")

# ---------------- GOOGLE SHEETS (YOUR EXACT) ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    if creds_json:
        client = gspread.service_account_from_dict(json.loads(creds_json))
    else:
        client = gspread.service_account(filename="credentials.json")
        
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:]  # Skip header
    print(f"✅ Connected. Processing {min(END_INDEX-START_INDEX+1, len(data_rows))} symbols")
except Exception as e:
    print(f"❌ Connection Error: {e}")
    raise

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SINGLE CHROME (🚀 SPEED FIX) ---------------- #
def setup_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-images")  # 🔥 30% faster
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.implicitly_wait(5)
    return driver

# ---------------- YOUR SCRAPER OPTIMIZED (SINGLE DRIVER) ---------------- #
driver = setup_driver()
wait = WebDriverWait(driver, 15)

def scrape_tradingview(url, symbol_name):
    if not url:
        print(f"  ❌ No URL for {symbol_name}")
        return ["N/A"] * 14
    
    try:
        print(f"  🌐 {symbol_name[:20]}...")
        
        # YOUR COOKIES (EXACT, once only)
        if os.path.exists("cookies.json"):
            driver.get("https://www.tradingview.com/")
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
                for c in cookies[:15]:
                    try:
                        driver.add_cookie({
                            "name": c.get("name"), "value": c.get("value"),
                            "domain": c.get("domain", ".tradingview.com"), 
                            "path": c.get("path", "/")
                        })
                    except: pass
            driver.refresh()
            time.sleep(2)  # Cookie settle
        
        # Navigate + YOUR EXACT WAIT
        driver.get(url)
        wait.until(EC.visibility_of_element_located((
            By.XPATH,
            '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
        )))
        
        # Scroll + short wait (YOUR TIMING)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
        time.sleep(1.5)
        
        # YOUR PROVEN SELECTOR (PRIMARY) + FALLBACKS
        soup = BeautifulSoup(driver.page_source, "html.parser")
        all_values = []
        
        # Strategy 1: YOUR EXACT CLASS (best match)
        els = soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        for el in els:
            text = el.get_text().replace('−', '-').replace('∅', '').strip()
            if text and len(text) < 25:
                all_values.append(text)
        
        # Strategy 2: Partial class match
        els = soup.find_all("div", class_=lambda x: x and "valueValue-l31H9iuA" in x)
        for el in els:
            text = el.get_text().replace('−', '-').replace('∅', '').strip()
            if text and len(text) < 25 and text not in all_values:
                all_values.append(text)
        
        # Strategy 3: Numeric fallback
        numeric_divs = soup.find_all('div', string=re.compile(r'[\d,.-]+'))
        for div in numeric_divs[:20]:
            text = div.get_text().strip().replace('−', '-')
            if re.match(r'^[\d,.-]+.*|.*[\d,.-]+$', text) and len(text) < 25 and text not in all_values:
                all_values.append(text)
        
        # Clean + pad to 14 (YOUR EXACT)
        unique_values = []
        for val in all_values:
            if val and len(val) > 0 and len(val) < 30 and val not in unique_values:
                unique_values.append(val)
        
        final_values = unique_values[:14]
        while len(final_values) < 14:
            final_values.append("N/A")
            
        print(f"  📊 {len(unique_values)} → {final_values[:3]}...")
        return final_values
        
    except TimeoutException:
        print(f"  ⏰ Timeout")
        return ["N/A"] * 14
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return ["N/A"] * 14

# ---------------- YOUR MAIN LOOP (BATCH=50) ---------------- #
batch = []
batch_start = None
processed = success_count = 0

print(f"\n🚀 Scraping {END_INDEX-START_INDEX+1} symbols → 16 cols (Name+Date+14vals)")

for i, row in enumerate(data_rows):
    if i < last_i or i < START_INDEX or i > END_INDEX:
        continue
    
    name = row[0].strip()
    url = row[3] if len(row) > 3 else ""
    target_row = i + 2
    
    if batch_start is None:
        batch_start = target_row
    
    print(f"[{i+1:4d}/{END_INDEX-START_INDEX+1}] {name[:25]} -> Row {target_row}")
    
    vals = scrape_tradingview(url, name)
    row_data = [name, current_date] + vals
    
    if any(v != "N/A" for v in vals):
        success_count += 1
    
    batch.append(row_data)
    processed += 1
    
    # 🔥 BATCH=50 (5x fewer writes)
    if len(batch) >= 50:
        try:
            dest_sheet.update(f"A{batch_start}", batch)
            print(f"💾 Rows {batch_start}-{target_row} (50×16 cols)")
            batch_start += len(batch)
            batch = []
        except Exception as e:
            print(f"❌ Write error: {e}")
    
    # YOUR CHECKPOINT
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(i + 1))
    
    time.sleep(0.8)  # Reduced from 1.8s

# Final batch
if batch and batch_start:
    try:
        dest_sheet.update(f"A{batch_start-len(batch)}", batch)
        print(f"💾 Final: Rows {batch_start-len(batch)}-{target_row}")
    except Exception as e:
        print(f"❌ Final write: {e}")

driver.quit()

print(f"\n🎉 COMPLETE!")
print(f"📊 Processed: {processed} | Success: {success_count}")
print(f"✅ Rate: {success_count/processed*100:.1f}%")
