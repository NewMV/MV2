print("🚀 Script started")

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
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
import traceback

# ---------------- ENV DEBUG ---------------- #
print("🔧 ENV CHECK")
print("START_INDEX:", os.getenv("START_INDEX"))
print("END_INDEX:", os.getenv("END_INDEX"))
print("SHARD_INDEX:", os.getenv("SHARD_INDEX"))
print("SHARD_STEP:", os.getenv("SHARD_STEP"))
print("GSPREAD_CREDENTIALS exists:", bool(os.getenv("GSPREAD_CREDENTIALS")))
print("TRADINGVIEW_COOKIES exists:", bool(os.getenv("TRADINGVIEW_COOKIES")))

# ---------------- SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "1"))
END_INDEX = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", "checkpoint_new_1.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else START_INDEX

print(f"📍 Starting from index: {last_i}")

# ---------------- CHROME SETUP ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")

# ---------------- GOOGLE SHEETS ---------------- #
print("🔐 Loading Google credentials...")
try:
    gc = gspread.service_account("credentials.json")
    print("✅ Google credentials loaded")
except Exception:
    print("❌ Failed to load credentials.json")
    traceback.print_exc()
    exit(1)

try:
    sheet_data = gc.open('New MV2').worksheet('Sheet1')
    print("✅ Google Sheet connected")
except Exception:
    print("❌ Failed to open Google Sheet")
    traceback.print_exc()
    exit(1)

# ---------------- READ EXCEL ---------------- #
print("📥 Fetching stock list from GitHub Excel...")
try:
    EXCEL_URL = "https://raw.githubusercontent.com/NewMV/MV2/main/Stock%20List%20%20(3).xlsx"
    response = requests.get(EXCEL_URL, timeout=30)
    response.raise_for_status()

    df = pd.read_excel(BytesIO(response.content), engine="openpyxl")
    name_list = df.iloc[:, 0].fillna("").tolist()
    company_list = df.iloc[:, 3].fillna("").tolist()

    print(f"✅ Loaded {len(company_list)} companies")
except Exception:
    print("❌ Excel load failed")
    traceback.print_exc()
    exit(1)

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(company_url):
    print("🌐 Launching Chrome...")
    driver = None
    try:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
        driver.set_window_size(1920, 1080)
        print("✅ Chrome launched")

        if os.path.exists("cookies.json"):
            print("🍪 Loading cookies...")
            driver.get("https://www.tradingview.com/")
            with open("cookies.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)

            for cookie in cookies:
                try:
                    cookie_to_add = {
                        "name": cookie["name"],
                        "value": cookie["value"],
                        "domain": cookie.get("domain", ".tradingview.com"),
                        "path": cookie.get("path", "/"),
                        "secure": cookie.get("secure", False),
                        "httpOnly": cookie.get("httpOnly", False)
                    }
                    driver.add_cookie(cookie_to_add)
                except Exception:
                    pass

            driver.refresh()
            time.sleep(2)
            print("✅ Cookies applied")
        else:
            print("⚠️ cookies.json missing")

        print("➡️ Opening company page:", company_url)
        driver.get(company_url)

        print("⏳ Waiting for data element...")
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((By.XPATH,
                '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
        )

        print("📄 Parsing page source")
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', '').strip()
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]

        print(f"📊 Extracted {len(values)} values")
        return values

    except Exception:
        print("❌ Error during scraping")
        traceback.print_exc()
        return []
    finally:
        if driver:
            driver.quit()
            print("🧹 Chrome closed")

# ---------------- MAIN LOOP ---------------- #
print("🔁 Starting main loop")

for i, company_url in enumerate(company_list[last_i:], last_i):
    if i < START_INDEX or i > END_INDEX:
        continue
    if i % SHARD_STEP != SHARD_INDEX:
        continue

    name = name_list[i] if i < len(name_list) else f"Row {i}"
    print(f"\n🧪 Scraping {i}: {name}")

    values = scrape_tradingview(company_url)

    if values:
        row = [name, current_date] + values
        try:
            sheet_data.append_row(row, table_range='A1')
            print("✅ Data saved to Google Sheet")
        except Exception:
            print("⚠️ Google Sheet append failed")
            traceback.print_exc()
    else:
        print("⚠️ No data scraped")

    with open(checkpoint_file, "w") as f:
        f.write(str(i))

    time.sleep(1)

print("🏁 Script finished")
