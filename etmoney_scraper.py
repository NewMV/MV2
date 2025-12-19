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

STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

CHUNK_START = int(os.getenv('CHUNK_START', 0))
CHUNK_END   = int(os.getenv('CHUNK_END', 2500))
BATCH_SIZE  = 20

SYMBOL_ETMONEY_MAP = {
    '360ONE': '360-one-wam-ltd/1035',
    '3IINFOLTD': '3i-infotech-ltd/348',
    '3MINDIA': '3m-india-ltd/1004',
    '5PAISA': '5paisa-capital-ltd/1005',
    '63MOONS': '63-moons-technologies-ltd/2781',
    'A2ZINFRA': 'a2z-infra-engineering-ltd/1007',
    '20MICRONS': '20-microns-ltd/2758',   # explicitly map your example [web:65]
}

def get_driver():
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
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def build_etmoney_url(symbol: str) -> str:
    base = "https://www.etmoney.com/stocks/"
    slug = SYMBOL_ETMONEY_MAP.get(symbol)
    if slug:
        return base + slug
    return base + f"{symbol.lower()}-ltd"

def extract_sector_badge(soup: BeautifulSoup) -> str:
    """
    Look only for header links whose href contains '/stocks/sector/'.
    That link corresponds to the sector pill like Mining/Minerals etc. [web:79][web:96][web:101]
    """
    header = soup.select_one("#page-container div.w-full.col-span-8")
    if not header:
        return "NO_DATA"

    # any <a> in header that links to a sector page
    for a in header.select("a[href*='/stocks/sector/']"):
        text = a.get_text(strip=True)
        if text:
            return text

    return "NO_DATA"

def get_sector(symbol: str, driver) -> str:
    try:
        url = build_etmoney_url(symbol)
        print(f"🌐 {symbol} → {url}")
        driver.get(url)
        WebDriverWait(driver, 15).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(2)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        sector = extract_sector_badge(soup)
        print(f"  → {sector}")
        return sector
    except Exception as e:
        print(f"❌ {symbol} error: {e}")
        return "NO_DATA"

def write_to_sheet6_ordered(client, results, chunk_start, local_index):
    try:
        sheet = client.open_by_url(NEW_MV2_URL).worksheet("Sheet6")
        start_row = chunk_start + local_index + 2
        end_row   = start_row + len(results) - 1
        sheet.update(f"A{start_row}:C{end_row}", results)
        print(f"✅ rows {start_row}-{end_row}")
        return True
    except Exception as e:
        print(f"❌ sheet write: {e}")
        return False

def main():
    driver = client = None
    print(f"🚀 ET Money sector-pill scraper {CHUNK_START}-{CHUNK_END}")
    try:
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        client = (
            gspread.service_account_from_dict(json.loads(creds_json))
            if creds_json else
            gspread.service_account(filename="credentials.json")
        )

        sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
        data = sheet.get_all_values()
        all_symbols = [r[0].strip().upper() for r in data[1:] if r and r[0].strip()]
        symbols = all_symbols[CHUNK_START:CHUNK_END]
        if not symbols:
            print("no symbols in this chunk")
            return

        backup = f"chunk_{CHUNK_START}_{CHUNK_END}_pill_{date.today().strftime('%d%m%Y')}.csv"
        with open(backup, "w", newline="") as f:
            csv.writer(f).writerow(["SYMBOL", "SECTOR_PILL", "DATE"])

        driver = get_driver()
        results = []
        local_index = 0

        for i, symbol in enumerate(symbols, 1):
            print(f"\n[{i}/{len(symbols)}] {symbol}")
            sector = get_sector(symbol, driver)
            results.append([symbol, sector, date.today().strftime("%d/%m/%Y")])

            if len(results) >= BATCH_SIZE:
                write_to_sheet6_ordered(client, results, CHUNK_START, local_index)
                with open(backup, "a", newline="") as f:
                    csv.writer(f).writerows(results)
                local_index += len(results)
                results = []
                time.sleep(random.uniform(2, 4))

        if results:
            write_to_sheet6_ordered(client, results, CHUNK_START, local_index)
            with open(backup, "a", newline="") as f:
                csv.writer(f).writerows(results)

        print("✅ done")
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    main()
