import os
import time
import json
import gspread
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- SHARDING (main logic preserved) ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "1"))
END_INDEX = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", "checkpoint_new_1.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else START_INDEX

# ---------------- SETUP ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# ---------------- GOOGLE SHEETS AUTH ---------------- #
# We use explicit naming here to avoid the Response [200] error
try:
    # 1. Authenticate
    client = gspread.service_account(filename="credentials.json")
    
    # 2. Open Source Spreadsheet (The file called "Stock List")
    # We use a unique variable name 'input_book' to avoid conflicts
    input_book = client.open("Stock List")
    source_sheet = input_book.worksheet("Sheet1")
    
    # 3. Open Destination Spreadsheet (The file called "New MV2")
    output_book = client.open("New MV2")
    destination_sheet = output_book.worksheet("Sheet5")
    
    # 4. Fetch all data from source
    all_rows = source_sheet.get_all_values()
    data_rows = all_rows[1:]  # Skip header
    
    print(f"✅ Connection Established.")
    print(f"📋 Source: 'Stock List' -> 'Sheet1'")
    print(f"📝 Destination: 'New MV2' -> 'Sheet5'")

except Exception as e:
    # If it still says Response [200], it means 'client.open' is failing 
    # and returning an error object.
    print(f"❌ Connection Error: {str(e)}")
    exit(1)

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SCRAPER FUNCTION (main logic preserved) ---------------- #
def scrape_tradingview(company_url):
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )
    driver.set_window_size(1920, 1080)
    try:
        if os.path.exists("cookies.json"):
            driver.get("https://www.tradingview.com/")
            with open("cookies.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for cookie in cookies:
                try:
                    cookie_to_add = {k: cookie[k] for k in ('name', 'value', 'domain', 'path') if k in cookie}
                    driver.add_cookie(cookie_to_add)
                except: pass
            driver.refresh()
            time.sleep(2)

        driver.get(company_url)
        print(f"🔎 Visiting: {company_url}")
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((By.XPATH, '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [el.get_text().replace('−', '-').replace('∅', '').strip() for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")]
        return values
    except Exception as e:
        print(f"❌ Scraping error: {e}")
        return []
    finally:
        driver.quit()

# ---------------- MAIN LOOP (sequence & compacting) ---------------- #
batch_data = []
# This tells the sheet exactly where to start writing the current batch
batch_start_row = None 

for i, row in enumerate(data_rows):
    if i < last_i or i < START_INDEX or i > END_INDEX:
        continue
    if i % SHARD_STEP != SHARD_INDEX:
        continue

    # Column A (index 0) = Name, Column D (index 3) = Link
    name = row[0] if len(row) > 0 else f"Unknown_{i}"
    company_url = row[3] if len(row) > 3 else ""
    target_row = i + 2 
    
    if batch_start_row is None:
        batch_start_row = target_row

    print(f"📌 [{i}] {name} -> Targeting Row {target_row}")
    scraped_values = scrape_tradingview(company_url)

    # SEQUENCE LOGIC: Always keep the name in Column A
    if scraped_values:
        final_row = [name, current_date] + scraped_values
    else:
        # Fill with "Error" so the sequence doesn't break
        final_row = [name, current_date, "Error", "Error", "Error", "Error"]

    batch_data.append(final_row)

    # COMPACT REQUESTS: Write every 5 items to avoid rate limits
    if len(batch_data) >= 5:
        try:
            destination_sheet.update(f'A{batch_start_row}', batch_data)
            print(f"💾 Saved batch to Row {target_row}")
            batch_data = []
            batch_start_row = None
        except Exception as e:
            print(f"⚠️ Batch Write Error: {e}")

    # Checkpoint (main logic)
    with open(checkpoint_file, "w") as f:
        f.write(str(i + 1))
    
    time.sleep(1)

# Final cleanup upload
if batch_data and batch_start_row:
    destination_sheet.update(f'A{batch_start_row}', batch_data)

print("\n🏁 Process finished.")
