import os, time, json, gspread, re
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


# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint.txt")


# Resume from checkpoint
last_i = START_INDEX
if os.path.exists(CHECKPOINT_FILE):
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            last_i = int(f.read().strip())
    except:
        pass

print(f"🔧 Range: {START_INDEX}-{END_INDEX} | Resume: {last_i}")


# ---------------- GOOGLE SHEETS ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    if creds_json:
        client = gspread.service_account_from_dict(json.loads(creds_json))
    else:
        client = gspread.service_account(filename="credentials.json")

    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:]  # skip header
    print(f"✅ Connected. Processing {END_INDEX-START_INDEX+1} symbols")
except Exception as e:
    print(f"❌ Connection Error: {e}")
    raise


current_date = date.today().strftime("%m/%d/%Y")
CHROME_SERVICE = Service(ChromeDriverManager().install())


# ------------- CREATE SHARED DRIVER ------------- #
def create_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(service=CHROME_SERVICE, options=opts)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )

    # Load cookies once
    if os.path.exists("cookies.json"):
        driver.get("https://www.tradingview.com/")
        try:
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
                for c in cookies[:15]:
                    try:
                        driver.add_cookie({
                            "name": c.get("name"),
                            "value": c.get("value"),
                            "domain": c.get("domain", ".tradingview.com"),
                            "path": c.get("path", "/")
                        })
                    except:
                        pass
            driver.refresh()
            time.sleep(2)  # light wait instead of 4s
        except Exception as e:
            print(f"⚠️ Cookie load issue: {e}")

    return driver


# ---------------- ALL 14 VALUES SCRAPER ---------------- #
def scrape_tradingview(driver, url, symbol_name):
    if not url:
        print(f"  ❌ No URL for {symbol_name}")
        return [""] * 14

    try:
        print(f"  🌐 {symbol_name[:20]}...")

        driver.set_page_load_timeout(60)
        driver.get(url)

        # Wait just until something is visible instead of big fixed sleeps
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, ".valueValue-l31H9iuA, div[class*='valueValue']")
                )
            )
        except TimeoutException:
            # fall back; still parse page_source
            print("  ⏰ Soft wait timeout, continuing with page_source")

        time.sleep(1.0)  # small buffer instead of 6s

        all_values = []

        selectors = [
            ".valueValue-l31H9iuA.apply-common-tooltip",
            ".valueValue-l31H9iuA",
            "div[class*='valueValue']",
            "div[class*='value']",
            ".chart-markup-table .value",
            "[data-value]"
        ]

        for selector in selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    values = []
                    for el in elements[:20]:
                        text = el.text.strip().replace('−', '-').replace('∅', '')
                        if text and len(text) < 25:
                            values.append(text)
                    all_values.extend(values)
                    if values:
                        print(f"  ✅ Selector '{selector}': {len(values)} values")
            except:
                continue

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        numeric_divs = soup.find_all('div', string=re.compile(r'[\d,.-]+'))
        for div in numeric_divs[:15]:
            text = div.get_text().strip().replace('−', '-')
            if re.match(r'^[\d,.-]+.*|.*[\d,.-]+$', text) and len(text) < 25:
                if text not in all_values:
                    all_values.append(text)

        tables = soup.find_all('table')
        for table in tables[:3]:
            for cell in table.find_all(['td', 'th'])[:20]:
                text = cell.get_text().strip().replace('−', '-')
                if re.match(r'[\d,.-]', text) and len(text) < 25 and text not in all_values:
                    all_values.append(text)

        unique_values = []
        for val in all_values:
            if val and len(val) > 0 and len(val) < 30 and val not in unique_values:
                unique_values.append(val)

        final_values = unique_values[:14]
        while len(final_values) < 14:
            final_values.append("N/A")

        print(f"  📊 {len(unique_values)} unique → {final_values[:3]}...")
        return final_values

    except TimeoutException:
        print("  ⏰ Timeout")
        return ["N/A"] * 14
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return ["N/A"] * 14


# ---------------- MAIN LOOP ---------------- #
batch = []
batch_start = None
processed = success_count = 0

print(f"\n🚀 Scraping {END_INDEX-START_INDEX+1} symbols → 14 columns each")

driver = create_driver()
try:
    for i, row in enumerate(data_rows):
        if i < last_i or i < START_INDEX or i > END_INDEX:
            continue

        name = row[0].strip()
        url = row[3] if len(row) > 3 else ""
        target_row = i + 2

        if batch_start is None:
            batch_start = target_row

        print(f"[{i+1:4d}/{END_INDEX-START_INDEX+1}] {name[:25]} -> Row {target_row}")

        vals = scrape_tradingview(driver, url, name)
        row_data = [name, current_date] + vals

        if any(v != "N/A" for v in vals):
            success_count += 1

        batch.append(row_data)
        processed += 1

        # bigger batch for faster gspread, same layout
        if len(batch) >= 20:
            try:
                dest_sheet.update(f"A{batch_start}", batch)
                print(f"💾 Rows {batch_start}-{target_row} ({len(batch)}×16 cols)")
                batch = []
                batch_start = None
                time.sleep(1.0)
            except Exception as e:
                print(f"❌ Write error: {e}")

        with open(CHECKPOINT_FILE, "w") as f:
            f.write(str(i + 1))

        time.sleep(0.4)  # lowered per-row delay to speed up

    if batch and batch_start:
        try:
            dest_sheet.update(f"A{batch_start}", batch)
            print(f"💾 Final: Rows {batch_start}-{target_row}")
        except Exception as e:
            print(f"❌ Final write: {e}")

finally:
    driver.quit()

print(f"\n🎉 COMPLETE!")
print(f"📊 Processed: {processed} | Success: {success_count}")
if processed > 0:
    print(f"✅ Success rate: {success_count/processed*100:.1f}%")
print(f"📍 Sheet5: Rows {START_INDEX+2}-{END_INDEX+2} × 16 columns")
