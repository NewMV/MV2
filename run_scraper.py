import time, json, requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

# ---------- config ----------

TEST_SYMBOLS = [
    "20MICRONS",
    "21STCENMGM",
    "360ONE",
    "3IINFOLTD",
    "3MINDIA",
    "3PLAND",
    "5PAISA",
    "63MOONS",
    "A2ZINFRA",
    "AAATECH",
]

SYMBOL_ETMONEY_MAP = {
    "20MICRONS": "20-microns-ltd/2758",
    "360ONE": "360-one-wam-ltd/1035",
    "3IINFOLTD": "3i-infotech-ltd/348",
    "3MINDIA": "3m-india-ltd/1004",
    "5PAISA": "5paisa-capital-ltd/1005",
    "63MOONS": "63-moons-technologies-ltd/2781",
    "A2ZINFRA": "a2z-infra-engineering-ltd/1007",
}

# ---------- helpers ----------

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

def get_nse_sector_api(symbol):
    try:
        url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Referer": "https://www.nseindia.com/"
        }
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json()
            return data.get("info", {}).get("industry") or data.get("info", {}).get("sector")
    except:
        return None

def debug_header_pills(soup, symbol):
    header = soup.select_one("#page-container div.w-full.col-span-8")
    if not header:
        print(f"  [DEBUG] {symbol}: NO HEADER")
        return
    print(f"  [DEBUG] {symbol}: HEADER PILLS")
    for a in header.select("a"):
        href = a.get("href")
        text = a.get_text(strip=True)
        print(f"     href={href} | text={text}")

def extract_sector_from_soup(soup, symbol):
    header = soup.select_one("#page-container div.w-full.col-span-8")
    if not header:
        return "NO_DATA"

    for a in header.select("a[href^='/stocks/sector/']"):
        href = a.get("href") or ""
        text = a.get_text(strip=True)
        last = href.rstrip("/").split("/")[-1]
        if text and last.isdigit():
            return text

    return "NO_DATA"

def get_sector_for_symbol(driver, symbol):
    url = build_etmoney_url(symbol)
    print(f"\n=== {symbol} ===")
    print("URL:", url)
    driver.get(url)

    WebDriverWait(driver, 20).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )

    # Try Selenium pills
    try:
        locator = (By.CSS_SELECTOR, "#page-container div.w-full.col-span-8 a[href^='/stocks/sector/']")
        elems = WebDriverWait(driver, 8).until(
            EC.presence_of_all_elements_located(locator)
        )
        for el in elems:
            href = el.get_attribute("href") or ""
            text = el.text.strip()
            path = "/" + "/".join(href.split("/", 3)[3:]) if "://" in href else href
            last = path.rstrip("/").split("/")[-1]
            print("  [SEL] href=", path, "| text=", text)
            if text and last.isdigit():
                print("  → chosen (Selenium):", text)
                return text
    except Exception as e:
        print("  [SEL] no sector pill via Selenium:", e)

    # Fallback: BS + debug
    soup = BeautifulSoup(driver.page_source, "html.parser")
    debug_header_pills(soup, symbol)
    sector = extract_sector_from_soup(soup, symbol)
    if sector != "NO_DATA":
        print("  → chosen (BS):", sector)
        return sector

    # Final fallback: NSE
    api_sector = get_nse_sector_api(symbol)
    print("  [NSE] sector:", api_sector)
    return api_sector or "NO_DATA"

# ---------- main test ----------

def main():
    driver = get_driver()
    try:
        for sym in TEST_SYMBOLS:
            sec = get_sector_for_symbol(driver, sym)
            print("RESULT:", sym, "→", sec)
            time.sleep(2)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
