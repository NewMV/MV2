from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from bs4 import BeautifulSoup
import gspread
from datetime import date
import os
import time
import json
import pandas as pd
import requests
from io import BytesIO
from webdriver_manager.chrome import ChromeDriverManager
import random

# ---------------- SHARDING (env-driven) ---------------- #
# Keeping this logic intact for future multi-job use
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "1"))
END_INDEX = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", "checkpoint_new_1.txt")

# Define last_i before first use in the code
last_i = 0 
try:
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            content = f.read().strip()
            # Ensure checkpoint value is an integer and within valid bounds
            if content.isdigit():
                last_i = int(content)
                if last_i < START_INDEX:
                    last_i = START_INDEX
            else:
                last_i = START_INDEX
    else:
        last_i = START_INDEX
except Exception as e:
    print(f"⚠️ Warning: Checkpoint file reading error: {e}. Starting from index {START_INDEX}.")
    last_i = START_INDEX


# ---------------- SETUP ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")


# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    gc = gspread.service_account("credentials.json")
    print("✅ Google Service Account authenticated successfully.")
except Exception as e:
    print(f"❌ FATAL ERROR: Could not load credentials.json. Error: {e}")
    exit(1)

# --- WRITING TARGET (New MV2, Sheet1) ---
SPREADSHEET_NAME = 'New MV2'
WORKSHEET_NAME = 'Sheet1'
try:
    spreadsheet = gc.open(SPREADSHEET_NAME)
    sheet_data = spreadsheet.worksheet(WORKSHEET_NAME)
    print(f"✅ Target sheet set to: '{SPREADSHEET_NAME}' -> '{WORKSHEET_NAME}'")
except gspread.exceptions.SpreadsheetNotFound:
    # Trigger image to help user fix permissions/typo
    print(f"❌ ERROR: Spreadsheet not found. Check name/sharing: '{SPREADSHEET_NAME}'")
    
    exit(1)
except gspread.exceptions.WorksheetNotFound:
    print(f"❌ ERROR: Worksheet not found inside '{SPREADSHEET_NAME}'. Check name: '{WORKSHEET_NAME}'")
    exit(1)
except Exception as e:
    print(f"❌ FATAL ERROR: Failed to open sheet/worksheet. Check permissions/typos. Details: {e}")
    exit(1)


# ---------------- READ STOCK LIST FROM GITHUB EXCEL ---------------- #
print("📥 Fetching stock list from GitHub Excel...")

try:
    EXCEL_URL ="https://raw.githubusercontent.com/NewMV/MV2/main/Stock%20List%20.xlsx" 
    response = requests.get(EXCEL_URL)
    response.raise_for_status()

    df = pd.read_excel(BytesIO(response.content), engine="openpyxl")
    name_list = df.iloc[:, 0].fillna("").tolist()    # Column A - Name (index 0)
    company_list = df.iloc[:, 3].fillna("").tolist() # Reads Column D - URL (index 3)

    print(f"✅ Loaded {len(company_list)} companies from GitHub Excel.")
except Exception as e:
    print(f"❌ FATAL ERROR: Error reading Excel from GitHub URL: {e}")
    exit(1)

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SCRAPER FUNCTION ---------------- #
def scrape_tradingview(company_url):
    driver = None
    try:
        print("⚙️ Setting up Chrome driver...")
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.set_window_size(1920, 1080)
        
        # LOGIN USING SAVED COOKIES
        if os.path.exists("cookies.json"):
            driver.get("https://www.tradingview.com/")
            with open("cookies.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)
            
            for cookie in cookies:
                try:
                    cookie_to_add = {k: cookie[k] for k in ('name', 'value', 'domain', 'path') if k in cookie}
                    cookie_to_add['secure'] = cookie.get('secure', False)
                    cookie_to_add['httpOnly'] = cookie.get('httpOnly', False)
                    if 'expiry' in cookie and cookie['expiry'] not in [None, '']:
                        cookie_to_add['expiry'] = int(cookie['expiry'])
                        
                    driver.add_cookie(cookie_to_add)
                except Exception as ce:
                    print(f"❌ DEBUG: Failed to add cookie {cookie.get('name', 'UNKNOWN')}. Error: {ce}")
                    
            driver.refresh()
            time.sleep(2)
        else:
            print("⚠️ cookies.json not found. Proceeding without login may limit data.")

        print(f"🌐 Navigating to URL: {company_url}")
        driver.get(company_url)
        
        # Wait until data element is visible
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((By.XPATH,
                '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
        )
        print("🔍 Data element found. Parsing page source...")

        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', '').strip()
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        return values

    except WebDriverException as we:
        print(f"❌ WEBDRIVER ERROR: Failed to start Chrome/navigate. Is the environment correct? Error: {we}")
        return []
    except NoSuchElementException:
        print(f"❌ SCRAPE ERROR: Data element not found on page: {company_url}")
        return []
    except TimeoutException:
        print(f"❌ SCRAPE ERROR: Timeout (45s) waiting for data on URL: {company_url}")
        return []
    except Exception as e:
        print(f"❌ UNEXPECTED SCRAPE ERROR for {company_url}: {e}")
        return []
    finally:
        if driver:
            driver.quit()

# ---------------- MAIN LOOP ---------------- #
# FIX APPLIED HERE: Correct syntax for list slicing: company_list[last_i:]
for i, company_url in enumerate(company_list[last_i:], last_i):
    # Sharding logic ensures only assigned segment runs
    if i < START_INDEX or i > END_INDEX:
        continue
    if i % SHARD_STEP != SHARD_INDEX:
        continue

    name = name_list[i] if i < len(name_list) else f"Row {i}"
    print(f"\n--- Processing Row {i} | {name} ---")

    values = scrape_tradingview(company_url)
    
    if values:
        # Row structure: [Col A: Name, Col B: Date, Col C: "", Col D: Value 1, ...]
        row = [name, current_date, ""] + values
        
        try:
            print(f"☁️ Attempting to append data to Google Sheet...")
            sheet_data.append_row(row, table_range='A1')
            print(f"✅ Successfully scraped and saved data for {name}.")
        except Exception as e:
            print(f"⚠️ FAILED to append data for {name}. GSpread Error: {e}")
            
    else:
        print(f"⚠️ Skipping {name}: No data was successfully scraped.")

    # Write checkpoint
    with open(checkpoint_file, "w") as f:
        f.write(str(i))

    # Sleep with jitter for rate limit avoidance
    sleep_time = 1.0 + random.random() * 0.5 
    time.sleep(sleep_time)

print("\nScraping job finished.")
