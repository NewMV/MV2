import os
import time
import json
import traceback
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

# ================== CONFIG & SHARDING ================== #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "1"))
END_INDEX = int(os.getenv("END_INDEX", "2500"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint_new_1.txt")

if os.path.exists(CHECKPOINT_FILE):
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            last_i = int(f.read().strip())
    except ValueError:
        # If file is corrupted or empty, start from START_INDEX
        last_i = START_INDEX
else:
    last_i = START_INDEX

current_date = date.today().strftime("%m/%d/%Y")

# ================== SELENIUM SETUP (ONE DRIVER) ================== #
def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )
    driver.set_page_load_timeout(60)
    return driver

def load_cookies_if_available(driver, cookies_path="cookies.json"):
    """
    Load cookies once per driver session if cookies.json exists.
    This helps keep you logged in to TradingView and speeds up scraping.
    """
    if not os.path.exists(cookies_path):
        return

    try:
        driver.get("https://www.tradingview.com/")
        with open(cookies_path, "r", encoding="utf-8") as f:
            cookies = json.load(f)

        for cookie in cookies:
            try:
                cookie_to_add = {
                    k: cookie[k]
                    for k in ("name", "value", "domain", "path")
                    if k in cookie
                }
                driver.add_cookie(cookie_to_add)
            except Exception:
                # Ignore individual cookie failures
                pass

        driver.refresh()
        time.sleep(2)
        print("✅ Cookies loaded and session refreshed.")
    except Exception as e:
        print(f"⚠️ Failed to load cookies properly: {e}")
        # Continue without cookies


# ================== GOOGLE SHEETS AUTH ================== #
def connect_sheets():
    """
    Connects to Google Sheets and returns (source_sheet, destination_sheet).
    Raises exception with clear explanation if anything fails.
    """
    try:
        client = gspread.service_account(filename="credentials.json")
    except Exception as e:
        raise RuntimeError(
            f"Failed to authenticate with Google Sheets using 'credentials.json'. "
            f"Check that the file exists, JSON is valid, and the service account "
            f"has proper permissions. Original error: {e}"
        )

    try:
        input_book = client.open("Stock List")
        source_sheet = input_book.worksheet("Sheet1")
    except Exception as e:
        raise RuntimeError(
            "Failed to open source spreadsheet 'Stock List' -> 'Sheet1'. "
            "Possible causes: wrong spreadsheet name, wrong worksheet name, "
            "or the service account email does not have access. "
            f"Original error: {e}"
        )

    try:
        output_book = client.open("New MV2")
        destination_sheet = output_book.worksheet("Sheet5")
    except Exception as e:
        raise RuntimeError(
            "Failed to open destination spreadsheet 'New MV2' -> 'Sheet5'. "
            "Possible causes: wrong spreadsheet name, wrong worksheet name, "
            "or the service account email does not have access. "
            f"Original error: {e}"
        )

    print("✅ Connection Established.")
    print("📋 Source: 'Stock List' -> 'Sheet1'")
    print("📝 Destination: 'New MV2' -> 'Sheet5'")
    return source_sheet, destination_sheet


# ================== SCRAPER FUNCTION ================== #
def scrape_tradingview(driver, company_url, index_for_log=None):
    """
    Scrapes values from a given TradingView URL using an existing Selenium driver.

    Returns:
        list of scraped values (strings). If error, returns [] and prints
        a clear, human-readable message including the row index if provided.
    """
    if not company_url:
        print(f"❌ Empty URL at index {index_for_log}. Skipping.")
        return []

    try:
        driver.get(company_url)
        print(f"🔎 Visiting: {company_url}")

        # Wait for the container that holds the values.
        # If TradingView structure changes, this is the first place to debug.
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((
                By.XPATH,
                '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
            ))
        )

        soup = BeautifulSoup(driver.page_source, "html.parser")
        elements = soup.find_all(
            "div",
            class_="valueValue-l31H9iuA apply-common-tooltip"
        )

        values = [
            el.get_text().replace('−', '-').replace('∅', '').strip()
            for el in elements
        ]

        if not values:
            print(
                f"⚠️ No values found at index {index_for_log}. "
                "Possible reasons: selector changed, page layout updated, "
                "or this URL does not show the expected widget."
            )

        return values

    except Exception as e:
        print(
            f"❌ Scraping error at index {index_for_log}: {e}\n"
            "    This usually means either:\n"
            "    - The TradingView page structure has changed (XPATH/class invalid), or\n"
            "    - The page didn't load correctly (network/timeout), or\n"
            "    - You hit a rate limit / anti-bot protection.\n"
            "    Full traceback:"
        )
        traceback.print_exc()
        return []


# ================== CHECKPOINT HELPER ================== #
def save_checkpoint(i):
    """
    Save the last processed index to the checkpoint file.
    """
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(str(i))
    except Exception as e:
        print(f"⚠️ Failed to write checkpoint file '{CHECKPOINT_FILE}': {e}")


# ================== MAIN ================== #
def main():
    # Connect to Sheets
    try:
        source_sheet, destination_sheet = connect_sheets()
    except RuntimeError as e:
        print(f"❌ Connection Error: {e}")
        return

    # Fetch all data once
    try:
        all_rows = source_sheet.get_all_values()
    except Exception as e:
        print(
            "❌ Failed to read data from 'Stock List' -> 'Sheet1'. "
            "Check API quota, network, or worksheet permissions. "
            f"Original error: {e}"
        )
        return

    data_rows = all_rows[1:]  # Skip header row

    # Create a single Selenium driver for all rows
    driver = None
    try:
        driver = create_driver()
        load_cookies_if_available(driver)
    except Exception as e:
        print(
            f"❌ Failed to start Selenium ChromeDriver. "
            f"Check Chrome installation or webdriver-manager logs. Error: {e}"
        )
        return

    batch_data = []
    batch_start_row = None  # Row index in Sheet where the batch starts

    try:
        for i, row in enumerate(data_rows, start=1):  # i is now 1-based for clarity
            # Apply checkpoints and sharding on i (1-based index of data_rows)
            if i < last_i or i < START_INDEX or i > END_INDEX:
                continue
            if (i - 1) % SHARD_STEP != SHARD_INDEX:
                # Use (i-1) because original logic used enumerate starting at 0
                continue

            # Column A (0) = Name, Column D (3) = Link
            name = row[0] if len(row) > 0 and row[0].strip() else f"Unknown_{i}"
            company_url = row[3] if len(row) > 3 else ""
            target_row = i + 1  # +1 because sheet has header at row 1

            if batch_start_row is None:
                batch_start_row = target_row

            print(f"\n📌 [{i}] {name} -> Targeting Sheet Row {target_row}")

            scraped_values = scrape_tradingview(
                driver,
                company_url,
                index_for_log=i
            )

            if scraped_values:
                final_row = [name, current_date] + scraped_values
            else:
                # Mark as error but keep sequence intact
                final_row = [name, current_date, "Error", "Error", "Error", "Error"]

            batch_data.append(final_row)

            # Batch write to Google Sheets every 5 rows (can adjust to 10/20).
            if len(batch_data) >= 5:
                try:
                    destination_sheet.update(
                        f"A{batch_start_row}",
                        batch_data
                    )
                    print(f"💾 Saved batch starting at Row {batch_start_row}")
                    batch_data = []
                    batch_start_row = None
                except Exception as e:
                    print(
                        "⚠️ Batch Write Error while writing to Google Sheets.\n"
                        "    Possible reasons:\n"
                        "    - API rate limit exceeded.\n"
                        "    - Network issues.\n"
                        "    - Invalid range or values.\n"
                        f"    Original error: {e}"
                    )
                    # Do not clear batch_data so you can retry manually if needed.

            # Save checkpoint after each row
            save_checkpoint(i + 1)

            # Small delay to be nice with TradingView + Google
            time.sleep(1)

        # Final upload if anything remains in batch_data
        if batch_data and batch_start_row is not None:
            try:
                destination_sheet.update(
                    f"A{batch_start_row}",
                    batch_data
                )
                print(f"💾 Final batch saved starting at Row {batch_start_row}")
            except Exception as e:
                print(
                    "⚠️ Final Batch Write Error.\n"
                    "    The last few rows were not written.\n"
                    "    You can rerun from the last checkpoint.\n"
                    f"    Original error: {e}"
                )

        print("\n🏁 Process finished.")

    finally:
        if driver is not None:
            driver.quit()


if __name__ == "__main__":
    main()
