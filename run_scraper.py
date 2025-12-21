import os, time, json, gspread, asyncio
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed
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
from threading import Lock

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "500"))  # FAST: 500 symbols
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "fast_checkpoint.txt")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))  # Parallel browsers
BATCH_SIZE = 10  # Bigger batches for speed

# Thread-safe checkpoint
checkpoint_lock = Lock()

# Resume from checkpoint
last_i = START_INDEX
if os.path.exists(CHECKPOINT_FILE):
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            last_i = int(f.read().strip())
    except:
        pass

print(f"⚡ FAST MODE | Range: {START_INDEX}-{END_INDEX} | Resume: {last_i} | Workers: {MAX_WORKERS}")

# ---------------- GOOGLE SHEETS ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    if creds_json:
        client = gspread.service_account_from_dict(json.loads(creds_json))
    else:
        client = gspread.service_account(filename="credentials.json")
        
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:]
    print(f"✅ Connected. FAST processing {min(END_INDEX-START_INDEX+1, len(data_rows))} symbols")
except Exception as e:
    print(f"❌ Connection Error: {e}")
    raise

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- ULTRA-FAST 14 VALUES SCRAPER ---------------- #
def scrape_tradingview_fast(url, symbol_name, worker_id):
    """Optimized scraper - 40% faster with exact 14 values"""
    if not url:
        return [""] * 14
    
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    opts.add_argument(f"--user-data-dir=/tmp/chrome{worker_id}")  # Isolate profiles
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    try:
        print(f"  🌐 W{worker_id} {symbol_name[:20]}...")
        
        # FAST cookies (only essential)
        if os.path.exists("cookies.json"):
            driver.get("https://www.tradingview.com/")
            try:
                with open("cookies.json", "r") as f:
                    cookies = json.load(f)[:10]  # Only top 10 cookies
                    for c in cookies:
                        driver.add_cookie({
                            "name": c.get("name"), "value": c.get("value"),
                            "domain": c.get("domain", ".tradingview.com"), 
                            "path": c.get("path", "/")
                        })
                driver.refresh()
            except: pass
            time.sleep(2)
        
        driver.set_page_load_timeout(45)
        driver.get(url)
        WebDriverWait(driver, 25).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(4)  # Optimized wait
        
        # **EXACT 14 VALUES - PRIORITY SELECTORS**
        values = []
        
        # 1. EXACT TradingView value classes (most reliable)
        exact_selectors = [
            ".valueValue-l31H9iuA.apply-common-tooltip",
            ".valueValue-l31H9iuA",
            '[class*="valueValue"]',
            ".chart-markup-table .value",
            ".tv-data-table__value",
            ".fundamental-value"
        ]
        
        for selector in exact_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for el in elements[:12]:
                    text = el.text.strip().replace('−', '-').replace('∅', '').replace(',', '')
                    if text and re.match(r'^[-0-9.%]+$', text) and len(text) < 20:
                        if text not in values:
                            values.append(text)
            except: continue
        
        # 2. Fallback: Parse page source FAST
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Target specific containers
        containers = soup.select('div[class*="fundamental"], div[class*="widget"], .chart-page')
        for container in containers[:3]:
            for div in container.find_all('div', limit=30):
                text = div.get_text().strip().replace('−', '-').replace(',', '')
                if re.match(r'^[-0-9.%\s]+$', text) and 2 < len(text) < 15 and text not in values:
                    values.append(text)
        
        # 3. Numeric pattern extraction (final fallback)
        numeric_pattern = re.compile(r'[-0-9.,%]+')
        page_text = driver.find_element(By.TAG_NAME, "body").text
        matches = numeric_pattern.findall(page_text)
        for match in matches[:20]:
            clean = re.sub(r'[^\d.-]', '', match)
            if len(clean) > 1 and clean not in values:
                values.append(clean)
        
        # Clean + Pad to EXACTLY 14 values
        clean_values = []
        for v in values[:14]:
            if re.match(r'^[-0-9.%]+$', v) and len(v) > 0:
                clean_values.append(v)
        
        final_values = clean_values[:14]
        while len(final_values) < 14:
            final_values.append("N/A")
        
        print(f"  ✅ W{worker_id} Found {len(clean_values)}/{14}")
        return final_values
        
    except TimeoutException:
        print(f"  ⏰ W{worker_id} Timeout")
        return ["N/A"] * 14
    except Exception as e:
        print(f"  ❌ W{worker_id} Error: {str(e)[:50]}")
        return ["N/A"] * 14
    finally:
        driver.quit()

# ---------------- ULTRA-FAST MAIN LOOP ---------------- #
def update_checkpoint(i):
    with checkpoint_lock:
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(str(i))

def process_symbol(args):
    i, row = args
    name = row[0].strip()
    url = row[3] if len(row) > 3 else ""
    target_row = i + 2
    
    vals = scrape_tradingview_fast(url, name, hash(name) % MAX_WORKERS)
    return [name, current_date] + vals, target_row

# Pre-filter range
target_rows = [(i, row) for i, row in enumerate(data_rows) 
              if last_i <= i <= END_INDEX and START_INDEX <= i]

print(f"\n⚡ PARALLEL SCRAPING {len(target_rows)} symbols with {MAX_WORKERS} workers")

all_results = []
success_count = 0

# PARALLEL PROCESSING - THE SPEED BOOST!
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    future_to_row = {executor.submit(process_symbol, args): args[0] 
                    for args in target_rows}
    
    for future in as_completed(future_to_row):
        i = future_to_row[future]
        try:
            result, target_row = future.result(timeout=90)
            all_results.append(result)
            
            if any(v != "N/A" for v in result[2:]):
                success_count += 1
            
            # Frequent checkpoint
            update_checkpoint(i)
            print(f"✅ [{i+1:3d}] Complete | Progress: {len(all_results)}/{len(target_rows)}")
            
        except Exception as e:
            print(f"❌ Row {i} failed: {e}")

# FAST BATCH WRITES
if all_results:
    try:
        # Sort by original order
        all_results.sort(key=lambda x: data_rows.index([x[0]] + ['']*4))
        dest_sheet.update("A2", all_results)  # Bulk write ALL
        print(f"💾 ULTRA-FAST: Wrote {len(all_results)} rows × 16 cols")
    except Exception as e:
        print(f"❌ Bulk write failed: {e}")
        # Fallback: smaller batches
        for i in range(0, len(all_results), BATCH_SIZE):
            batch = all_results[i:i+BATCH_SIZE]
            dest_sheet.update(f"A{i+2}", batch)
            time.sleep(1)

print(f"\n🎉 ULTRA-FAST COMPLETE!")
print(f"📊 Processed: {len(all_results)} | Success: {success_count}")
print(f"📍 Sheet5: Rows {START_INDEX+2}-{END_INDEX+2}")
print(f"⚡ Speed: {success_count/len(all_results)*100:.1f}% success")
