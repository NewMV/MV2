from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
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
chrome_options.add_argument("--remote-debugging-port=9222")
# Setting user agent to appear as a standard browser
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")


# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    gc = gspread.service_account("credentials.json")
except Exception as e:
    print(f"Error loading credentials.json: {e}")
    exit(1)

# --- WRITING TARGET (New MV2, sheet1) ---
try:
    sheet_data = gc.open('New MV2').worksheet('sheet1')
    print(f"✅ Target sheet set to: 'New MV2' -> 'sheet1'")
except Exception as e:
    print(f"❌ Error opening new sheet/worksheet: {e}")
    exit(1)


# ---------------- READ STOCK LIST FROM GITHUB EXCEL ---------------- #
print("📥 Fetching stock list from GitHub Excel...")

try:
    # --- UPDATED RAW URL ---
    EXCEL_URL ="https://raw.githubusercontent.com/NewMV/MV2/main/Stock%20List%20.xlsx" 
    response = requests.get(EXCEL_URL)
    response.raise_for_status()

    df = pd.read_excel(BytesIO(response.content), engine="openpyxl")
    name_list = df.iloc[:, 0].fillna("").tolist()    # Column A - Name (index 0)
    company_list = df.iloc[:, 3].fillna("").tolist() # Reads Column D - URL (index 3)

    print(f"✅ Loaded {len(company_list)} companies from GitHub Excel.")
except Exception as e:
    print(f"❌ Error reading Excel from GitHub: {e}")
    exit(1)

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SCRAPER FUNCTION ---------------- #
# Note: Driver is initialized inside the function and quit after each scrape, which
# is safe but can be slow due to repeated startup/shutdown.
def scrape_tradingview(company_url):
    try:
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
                    # Add expiry only if it exists and is not None/empty
                    if 'expiry' in cookie and cookie['expiry'] not in [None, '']:
                         cookie_to_add['expiry'] = int(cookie['expiry'])
                         
                    driver.add_cookie(cookie_to_add)
                except Exception:
                    pass
            driver.refresh()
            time.sleep(2)
        else:
            print("⚠️ cookies.json not found. Proceeding without login may limit data.")

        driver.get(company_url)
        
        # Wait until a specific key data element is visible (45 seconds timeout)
        # Using a longer, more specific XPath for robustness against simple class name changes
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((By.XPATH,
                '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
        )

        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Extract all value elements based on class name
        values = [
            el.get_text().replace('−', '-').replace('∅', '').strip()
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        return values

    except NoSuchElementException:
        print(f"Data element not found for URL: {company_url}")
        return []
    except TimeoutException:
        print(f"Timeout waiting for data on URL: {company_url}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred during scraping for {company_url}: {e}")
        return []
    finally:
        # Crucial: Always quit the driver to free up resources
        if 'driver' in locals() or 'driver' in globals():
            driver.quit()

# ---------------- MAIN LOOP ---------------- #
for i, company_url in enumerate(company_list[last_i:], last_i):
    if i < START_INDEX or i > END_INDEX:
        continue
    if i % SHARD_STEP != SHARD_INDEX:
        continue

    name = name_list[i] if i < len(name_list) else f"Row {i}"
    print(f"Scraping {i}: {name} | {company_url}")

    values = scrape_tradingview(company_url)
    
    if values:
        # --- MODIFIED: Insert "" placeholder to start scraped data in Column D ---
        # Row structure: [Col A: Name, Col B: Date, Col C: "", Col D: Value 1, ...]
        row = [name, current_date, ""] + values
        
        try:
            sheet_data.append_row(row, table_range='A1')
            print(f"✅ Successfully scraped and saved data for {name}, starting in Column D.")
        except Exception as e:
            print(f"⚠️ Failed to append for {name}: {e}")
    else:
        print(f"⚠️ Skipping {name}: No data scraped.")

    # Write checkpoint
    with open(checkpoint_file, "w") as f:
        f.write(str(i))

    # Sleep with jitter for rate limit avoidance
    sleep_time = 1.0 + random.random() * 0.5 # Sleeps between 1.0 and 1.5 seconds
    time.sleep(sleep_time)

print("Scraping job finished.")
