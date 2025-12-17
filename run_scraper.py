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

# ---------------- SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "1"))
END_INDEX = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", "checkpoint_new_1.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else START_INDEX

# ---------------- CHROME SETUP ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")

# ---------------- GOOGLE SHEETS ---------------- #
gc = gspread.service_account("credentials.json")
sheet_data = gc.open('New MV2').worksheet('Sheet5')

# ---------------- LOAD EXCEL FROM GITHUB ---------------- #
EXCEL_URL = "https://raw.githubusercontent.com/NewMV/MV2/main/Stock%20List%20%20(3).xlsx"
df = pd.read_excel(BytesIO(requests.get(EXCEL_URL).content), engine="openpyxl")
name_list = df.iloc[:, 0].fillna("").tolist()
company_list = df.iloc[:, 3].fillna("").tolist()

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SCRAPER ---------------- #
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
                for c in json.load(f):
                    try:
                        driver.add_cookie({
                            k: c[k] for k in c
                            if k in ("name", "value", "domain", "path")
                        })
                    except:
                        pass
            driver.refresh()
            time.sleep(2)

        driver.get(company_url)

        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located(
                (By.CLASS_NAME, "valueValue-l31H9iuA")
            )
        )

        soup = BeautifulSoup(driver.page_source, "html.parser")
        return [
            el.get_text().replace('−', '-').replace('∅', '').strip()
            for el in soup.find_all(
                "div", class_="valueValue-l31H9iuA apply-common-tooltip"
            )
        ]

    except Exception:
        return []
    finally:
        driver.quit()

# ---------------- MAIN LOOP ---------------- #
for i, company_url in enumerate(company_list[last_i:], last_i):

    if i < START_INDEX or i > END_INDEX:
        continue

    if i % SHARD_STEP != SHARD_INDEX:
        continue

    name = name_list[i] if i < len(name_list) else f"Row {i}"
    print(f"📌 {i} → {name}")

    values = scrape_tradingview(company_url)

    row_data = [name, current_date] + values if values else ["ERROR"]

    sheet_row = i + 2  # header offset
    range_name = f"A{sheet_row}:ZZ{sheet_row}"

    try:
        sheet_data.update(range_name, [row_data], value_input_option="RAW")
        print(f"✅ Written to row {sheet_row}")
    except Exception as e:
        print(f"⚠️ Sheet write error: {e}")

    with open(checkpoint_file, "w") as f:
        f.write(str(i))

    time.sleep(1)

print("✅ DONE")
