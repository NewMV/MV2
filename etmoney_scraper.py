import os, time, json, gspread, random, csv
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
import requests

STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

CHUNK_START = int(os.getenv('CHUNK_START', 0))
CHUNK_END   = int(os.getenv('CHUNK_END', 2500))
BATCH_SIZE  = 20

# If some symbols need custom slug, keep this. Otherwise it will use {symbol}-ltd.
SYMBOL_ETMONEY_MAP = {
    '360ONE': '360-one-wam-ltd/1035',
    '3IINFOLTD': '3i-infotech-ltd/1003',
    '3MINDIA': '3m-india-ltd/1004',
    '5PAISA': '5paisa-capital-ltd/1005',
    '63MOONS': '63-moons-technologies-ltd/1006',
    'A2ZINFRA': 'a2z-infra-engineering-ltd/1007',
}

# 🎯 CSS selector for pill badge next to NSE symbol (Mining/Minerals etc.)
# Note the escaped ".5" in the class name.
BADGE_SELECTOR = (
    "#page-container > "
    "div.bg-white-color.w-full.mt-4.mb-3\\.5 > "
    "div > div > "
    "div.w-full.col-span-8 > "
    "div > div > div > a"
)

# Optional shorter backup selector (kept but only used if first fails)
BADGE_SELECTOR_SHORT = "div.w-full.col-span-8 div > div > div > a"

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def build_etmoney_url(symbol: str) -> str:
    """
    For 20MICRONS, ET Money URL is like:
    https://www.etmoney.com/stocks/20-microns-ltd/2758  [web:65]
    For most stocks, pattern is {slug or symbol-ltd}/<numeric-id>.
    If you don't know ids, you can omit them and still get the header:
    https://www.etmoney.com/stocks/20-microns-ltd
    """
    base = "https://www.etmoney.com/stocks/"
    slug = SYMBOL_ETMONEY_MAP.get(symbol)
    if slug:
        return base + slug
    # generic fallback slug
    return base + f"{symbol.lower()}-ltd"

def get_pill_badge_from_soup(soup: BeautifulSoup) -> str:
    """
    Only read the pill badge <a> element that shows Mining/Minerals type label.
    """
    el = soup.select_one(BADGE_SELECTOR)
    if el and el.get_text(strip=True):
        return el.get_text(strip=True)

    # Backup shorter selector (in case layout changes slightly)
    el = soup.select_one(BADGE_SELECTOR_SHORT)
    if el and el.get_text(strip=True):
        return el.get_text(strip=True)

    return "NO_DATA"

def get_pill_badge_selenium(driver, symbol: str) -> str:
    """
    Open ET Money stock page and grab ONLY the pill badge text via CSS selector.
    """
    try:
        url = build_etmoney_url(symbol)
        print(f"🌐 {symbol} → {url}")
        driver.get(url)

        # Wait for page load
        WebDriverWait(driver, 15).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(2)  # extra wait for React layout

        # First try with pure Selenium (dynamic content safe)
        try:
            el = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, BADGE_SELECTOR))
            )
            text = el.text.strip()
            if text:
                print(f"  ✅ badge: {text}")
                return text
        except Exception:
            print(f"  ⚠ exact badge selector not found, trying BeautifulSoup for {symbol}")

        # Fallback: BeautifulSoup on page_source
        soup = BeautifulSoup(driver.page_source, "html.parser")
        badge = get_pill_badge_from_soup(soup)
        if badge != "NO_DATA":
            print(f"  ✅ BS badge: {badge}")
            return badge

    except Exception as e:
        print(f"  ❌ error for {symbol}: {e}")

    return "NO_DATA"

def get_sector(symbol: str, driver) -> str:
    """
    Only return pill badge; do NOT use NSE API or regex industry.
    """
    return get_pill_badge_selenium(driver, symbol)

def write_to_sheet6_ordered(client, results, chunk_start, local_index):
    """
    Write rows to Sheet6 exactly aligned with source Sheet1 indices.
    """
    try:
        sheet = client.open_by_url(NEW_MV2_URL).worksheet("Sheet6")
        start_row = chunk_start + local_index + 2   # +2 because Sheet1 header row
        end_row   = start_row + len(results) - 1
        range_name = f"A{start_row}:C{end_row}"
        sheet.update(range_name, results)
        print(f"✅ Written rows {start_row}-{end_row} ({len(results)} rows)")
        return True
    except Exception as e:
        print(f"❌ Google Sheet write failed: {e}")
        return False

def main():
    driver = None
    client = None
    print(f"🚀 ET Money pill-badge scraper | Chunk {CHUNK_START}-{CHUNK_END}")

    try:
        # Google Sheets auth
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        if creds_json:
            client = gspread.service_account_from_dict(json.loads(creds_json))
        else:
            client = gspread.service_account(filename="credentials.json")

        # Load full symbol list from STOCK_LIST_URL Sheet1
        source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
        all_data = source_sheet.get_all_values()
        all_symbols = [
            row[0].strip().upper()
            for row in all_data[1:]
            if row and row[0].strip()
        ]

        symbols = all_symbols[CHUNK_START:CHUNK_END]
        if not symbols:
            print("No symbols found in this chunk, exiting.")
            return

        print(f"📖 {len(symbols)} symbols: {symbols[0]} → {symbols[-1]}")

        # CSV backup
        chunk_file = f"chunk_{CHUNK_START}_{CHUNK_END}_pill_badge_{date.today().strftime('%d%m%Y')}.csv"
        with open(chunk_file, "w", newline="") as f:
            csv.writer(f).writerow(["SYMBOL", "SECTOR_BADGE", "DATE"])

        driver = get_driver()
        results = []
        local_index = 0

        for i, symbol in enumerate(symbols, 1):
            print(f"\n[{i:3d}/{len(symbols)}] {symbol}")
            sector = get_sector(symbol, driver)
            results.append([symbol, sector, date.today().strftime("%d/%m/%Y")])

            if len(results) >= BATCH_SIZE:
                # Write to sheet
                write_to_sheet6_ordered(client, results, CHUNK_START, local_index)
                # Append to CSV
                with open(chunk_file, "a", newline="") as f:
                    csv.writer(f).writerows(results)
                local_index += len(results)
                results = []
                time.sleep(random.uniform(2, 4))

        # Final batch
        if results:
            write_to_sheet6_ordered(client, results, CHUNK_START, local_index)
            with open(chunk_file, "a", newline="") as f:
                csv.writer(f).writerows(results)

        print(
            f"\n🎉 Done! {len(symbols)} symbols → Sheet6 rows "
            f"{CHUNK_START+2}-{CHUNK_START+len(symbols)+1}"
        )
        print(f"💾 Backup CSV: {chunk_file}")

    except Exception as e:
        print(f"💥 FATAL ERROR: {e}")
    finally:
        if driver:
            driver.quit()
        print("👋 Driver closed")

if __name__ == "__main__":
    main()
