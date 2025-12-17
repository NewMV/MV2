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

# ---------------- CONFIG (ONLY SHEET ACCESS CHANGED) ---------------- #
# Use the full URL or the ID of each sheet to avoid title issues.
# Get this from your browser address bar while the sheet is open.
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "1"))
END_INDEX = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", "checkpoint_new_1.txt")

if os.path.exists(checkpoint_file):
    try:
        with open(checkpoint_file, "r") as f:
            last_i = int(f.read().strip())
    except ValueError:
        last_i = START_INDEX
else:
    last_i = START_INDEX

# ---------------- SETUP ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# ---------------- GOOGLE SHEETS AUTH (UPDATED) ---------------- #
try:
    client = gspread.service_account(filename="credentials.json")

    # Instead of open("Stock List"), use open_by_url (or open_by_key).
    # This avoids title‑lookup issues and the confusing <Response 200> error.[web:31][web:53][web:62]
    input_book = client.open_by_url(STOCK_LIST_URL)
    source_sheet = input_book.worksheet("Sheet1")

    output_book = client.open_by_url(NEW_MV2_URL)
    destination_sheet = output_book.worksheet("Sheet5")

    all_rows = source_sheet.get_all_values()
    data_rows = all_rows[1:]  # Skip header

    print(f"✅ Connection Established.")
    print(f"📋 Source: 'Stock List' -> 'Sheet1'")
    print(f"📝 Destination: 'New MV2' -> 'Sheet5'")

except Exception as e:
    # If you still see something like <Response 200>, it usually means:
    # - URL/ID is wrong, or
    # - service account email still has no access, or
    # - worksheet name (Sheet1/Sheet5) is not exact.[web:21][web:23][web:31]
    print(
        "❌ Connection Error while opening Google Sheets.\n"
        "   Things to check:\n"
        "   1) STOCK_LIST_URL and NEW_MV2_URL are correct (copied from browser URL).\n"
        "   2) The service account 'client_email' from credentials.json has at least 'Editor' access\n"
        "      to both sheets (use the Share button in Google Sheets).\n"
        "   3) The tab names are exactly 'Sheet1' and 'Sheet5'.\n"
        f"   Original error: {e}"
    )
    raise

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SCRAPER FUNCTION (main logic preserved) ---------------- #
def scrape_tradingview(company_url):
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )
    driver.set_window_size(1920, 1080)
    try:
        # Load cookies if available (same as before)
        if os.path.exists("cookies.json"):
            driver.get("https://www.tradingview.com/")
            with open("cookies.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for cookie in cookies:
                try:
                    cookie_to_add = {
                        k: cookie[k]
                        for k in ('name', 'value', 'domain', 'path')
                        if k in cookie
                    }
                    driver.add_cookie(cookie_to_add)
                except:
                    pass
            driver.refresh()
            time.sleep(2)

        driver.get(company_url)
        print(f"🔎 Visiting: {company_url}")
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((
                By.XPATH,
                '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
            ))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', '').strip()
            for el in soup.find_all(
                "div",
                class_="valueValue-l31H9iuA apply-common-tooltip"
            )
        ]
        return values
    except Exception as e:
        print(
            f"❌ Scraping error: {e}\n"
            "   This usually means: page layout/XPATH changed, page didn't load, "
            "or TradingView blocked the request."
        )
        return []
    finally:
        driver.quit()

# ---------------- MAIN LOOP (logic preserved) ---------------- #
batch_data = []
batch_start_row = None  # This tells the sheet exactly where to start writing the current batch

for i, row in enumerate(data_rows):
    # Keep your original sharding + checkpoint logic
    if i < last_i or i < START_INDEX or i > END_INDEX:
        continue
    if i % SHARD_STEP != SHARD_INDEX:
        continue

    # Column A (index 0) = Name, Column D (index 3) = Link
    name = row[0] if len(row) > 0 else f"Unknown_{i}"
    company_url = row[3] if len(row) > 3 else ""
    target_row = i + 2  # +2 because of header row

    if batch_start_row is None:
        batch_start_row = target_row

    print(f"📌 [{i}] {name} -> Targeting Row {target_row}")
    scraped_values = scrape_tradingview(company_url)

    # Always keep the name in Column A
    if scraped_values:
        final_row = [name, current_date] + scraped_values
    else:
        # Fill with "Error" so the sequence doesn't break
        final_row = [name, current_date, "Error", "Error", "Error", "Error"]

    batch_data.append(final_row)

    # Compact requests: write every 5 items
    if len(batch_data) >= 5:
        try:
            destination_sheet.update(f'A{batch_start_row}', batch_data)
            print(f"💾 Saved batch to Row {target_row}")
            batch_data = []
            batch_start_row = None
        except Exception as e:
            print(
                "⚠️ Batch Write Error when writing to Google Sheets.\n"
                "   Likely reasons: API rate limit, invalid range, or network issue.\n"
                f"   Original error: {e}"
            )

    # Checkpoint (same logic, but slightly safer)
    try:
        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))
    except Exception as e:
        print(f"⚠️ Failed to write checkpoint file '{checkpoint_file}': {e}")

    time.sleep(1)

# Final cleanup upload
if batch_data and batch_start_row:
    try:
        destination_sheet.update(f'A{batch_start_row}', batch_data)
        print(f"💾 Final batch saved to Row {batch_start_row}")
    except Exception as e:
        print(
            "⚠️ Final Batch Write Error. The last few rows might not be saved.\n"
            f"   Original error: {e}"
        )

print("\n🏁 Process finished.")
