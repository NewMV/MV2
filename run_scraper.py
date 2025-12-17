import os
import time
import json
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
import gspread

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "1"))
END_INDEX = int(os.getenv("END_INDEX", "2500"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint.txt")
BATCH_SIZE = 5

# Load checkpoint
last_processed = START_INDEX - 1
if os.path.exists(CHECKPOINT_FILE):
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            last_processed = int(f.read().strip())
    except:
        pass

print(f"📍 Starting from index {last_processed + 1}")

# ---------------- GOOGLE SHEETS ---------------- #
try:
    client = gspread.service_account(filename="credentials.json")
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    
    # Read ALL data at once (skip header)
    all_rows = source_sheet.get_all_values()[1:]
    print(f"✅ Loaded {len(all_rows)} rows from source sheet")
except Exception as e:
    print(f"❌ Sheets Error: {e}")
    raise

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SCRAPER FUNCTION ---------------- #
def scrape_tradingview(url):
    if not url:
        return [""] * 6
    
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    try:
        # Load cookies if available
        if os.path.exists("cookies.json"):
            driver.get("https://www.tradingview.com/")
            try:
                with open("cookies.json", "r") as f:
                    cookies = json.load(f)
                    for cookie in cookies:
                        cookie_data = {k: cookie[k] for k in ('name', 'value', 'domain', 'path') if k in cookie}
                        driver.add_cookie(cookie_data)
                driver.refresh()
            except:
                pass

        driver.get(url)
        WebDriverWait(driver, 30).until(
            EC.visibility_of_element_located((By.XPATH, '//div[contains(@class, "valueValue")]'))
        )
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        cleaned_values = [el.get_text().replace('−', '-').replace('∅', '').strip() for el in values]
        
        # Ensure exactly 6 values (pad with empty if needed)
        return cleaned_values[:6] + [""] * max(0, 6 - len(cleaned_values))
        
    except Exception as e:
        print(f"⚠️ Scrape failed: {e}")
        return ["Error"] * 6
    finally:
        driver.quit()

# ---------------- MAIN BATCH PROCESSING ---------------- #
batch_data = []
batch_start_row = None
total_processed = 0

for i, row in enumerate(all_rows):
    row_index = i + 1  # 1-based index for sheets
    
    # Skip if not in range or wrong shard
    if (row_index <= last_processed or 
        row_index < START_INDEX or 
        row_index > END_INDEX or 
        row_index % SHARD_STEP != SHARD_INDEX):
        continue
    
    # Get symbol and URL (handle rows with insufficient columns)
    symbol = row[0] if row else ""
    url = row[3] if len(row) > 3 else ""
    
    print(f"🔎 [{row_index}] {symbol}")
    
    # Scrape data
    scraped_values = scrape_tradingview(url)
    
    # Create row: [Symbol, Date, scraped_data...]
    new_row = [symbol, current_date] + scraped_values
    batch_data.append(new_row)
    
    # Track batch start row (dest sheet row = source row + 1 for header)
    if batch_start_row is None:
        batch_start_row = row_index + 1
    
    total_processed += 1
    
    # Save checkpoint
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(row_index))
    
    # Write batch when full
    if len(batch_data) >= BATCH_SIZE:
        try:
            dest_sheet.update(f'A{batch_start_row}', batch_data)
            print(f"💾 Saved batch: rows {batch_start_row}-{batch_start_row + len(batch_data) - 1}")
            batch_data = []
            batch_start_row = None
        except Exception as e:
            print(f"❌ Batch save failed: {e}")
        
        time.sleep(1)  # Rate limit

# Final batch save
if batch_data:
    try:
        dest_sheet.update(f'A{batch_start_row}', batch_data)
        print(f"💾 Final batch saved: rows {batch_start_row}-{batch_start_row + len(batch_data) - 1}")
    except Exception as e:
        print(f"❌ Final batch save failed: {e}")

print(f"\n🏁 Done! Processed {total_processed} stocks. Checkpoint: {last_processed + total_processed}")
