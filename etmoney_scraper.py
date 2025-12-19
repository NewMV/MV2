import os, time, json, gspread, random
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
import re
import requests

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

# NSE → ET Money URL mapping (add more as needed)
SYMBOL_ETMONEY_MAP = {
    '360ONE': '360-one-wam-ltd/1035',
    '3IINFOLTD': '3i-infotech-ltd/1003', 
    '3MINDIA': '3m-india-ltd/1004',
    '5PAISA': '5paisa-capital-ltd/1005',
    '63MOONS': '63-moons-technologies-ltd/1006',
    'A2ZINFRA': 'a2z-infra-engineering-ltd/1007',
    # Add more: 'SYMBOL': 'etmoney-slug/id'
}

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def get_nse_sector_api(symbol):
    """FAST API fallback - NSE official data"""
    try:
        url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
        headers = {'User-Agent': 'Mozilla/5.0...'}
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return data.get('info', {}).get('industry', 'NSE_API')
    except:
        pass
    return None

def scrape_sector_direct(driver, symbol):
    """Method 1: Direct ET Money URL (FASTEST)"""
    try:
        slug = SYMBOL_ETMONEY_MAP.get(symbol)
        if slug:
            url = f"https://www.etmoney.com/stocks/{slug}"
            driver.get(url)
            time.sleep(3)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            sector = extract_sector(soup)
            if sector:
                return sector
        
        # Fallback: Try generic URL pattern
        generic_url = f"https://www.etmoney.com/stocks/{symbol.lower()}-ltd/1000"
        driver.get(generic_url)
        time.sleep(3)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        return extract_sector(soup)
    except:
        return None

def scrape_sector_search(driver, symbol):
    """Method 2: Search (backup)"""
    try:
        driver.get("https://www.etmoney.com/stocks")
        wait = WebDriverWait(driver, 10)
        
        # TRY ALL possible search selectors
        selectors = [
            "input[placeholder*='Search']",
            "input[placeholder*='stock']", 
            "input[placeholder*='company']",
            ".search-input", "[role='combobox']",
            "input[type='search']"
        ]
        
        search_box = None
        for sel in selectors:
            try:
                search_box = driver.find_element(By.CSS_SELECTOR, sel)
                break
            except:
                continue
        
        if search_box:
            search_box.clear()
            search_box.send_keys(symbol)
            time.sleep(2)
            search_box.send_keys(Keys.ENTER)
            time.sleep(4)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            return extract_sector(soup)
    except:
        pass
    return None

def extract_sector(soup):
    """Robust sector extraction"""
    text = soup.get_text()
    
    # Pattern matching
    patterns = [
        r'Sector[:\s]*([A-Z][A-Za-z\s\-&/]{2,50})',
        r'Industry[:\s]*([A-Z][A-Za-z\s\-&/]{2,50})',
        r'belongs to[:\s]*([A-Z][A-Za-z\s\-&/]{2,50})'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return match.group(1).strip()
    
    # Table scanning
    for table in soup.find_all('table')[:5]:  # First 5 tables
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                row_text = ' '.join(c.get_text(strip=True) for c in cells)
                if any(word in row_text.lower() for word in ['sector', 'industry']):
                    for cell in cells[1:]:
                        sector = cell.get_text(strip=True).strip()
                        if len(sector) > 2 and any(c.isalpha() for c in sector):
                            return sector
    return None

def get_sector(symbol, driver):
    """3-Step fallback system"""
    print(f"  🔍 Trying {symbol}")
    
    # Step 1: NSE API (fastest)
    sector = get_nse_sector_api(symbol)
    if sector:
        print(f"  ✅ NSE API: {sector}")
        return sector
    
    # Step 2: Direct URL
    sector = scrape_sector_direct(driver, symbol)
    if sector:
        print(f"  ✅ Direct URL: {sector}")
        return sector
    
    # Step 3: Search
    sector = scrape_sector_search(driver, symbol)
    if sector:
        print(f"  ✅ Search: {sector}")
        return sector
    
    return "NO_DATA"

def read_symbols(client):
    sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    all_data = sheet.get_all_values()
    symbols = [row[0].strip().upper() for row in all_data[1:] if row and row[0].strip()]
    print(f"📖 Read {len(symbols)} symbols")
    return symbols

def write_batch(client, batch_data):
    try:
        sheet = client.open_by_url(NEW_MV2_URL).worksheet("Sheet6")
        sheet.append_rows(batch_data)
        print(f"💾 Batch written: {len(batch_data)} rows")
        return True
    except Exception as e:
        print(f"❌ Batch failed: {e}")
        return False

def main():
    driver = None
    client = None
    BATCH_SIZE = 5
    
    try:
        # Auth
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        if creds_json:
            client = gspread.service_account_from_dict(json.loads(creds_json))
        elif os.path.exists("credentials.json"):
            client = gspread.service_account(filename="credentials.json")
        else:
            raise Exception("No credentials")
        
        symbols = read_symbols(client)
        driver = get_driver()
        
        batch = []
        for i, symbol in enumerate(symbols, 1):
            print(f"\n[{i}/{len(symbols)}] {symbol}")
            sector = get_sector(symbol, driver)
            print(f"   ✅ {sector}")
            
            batch.append([symbol, sector, date.today().strftime("%d/%m/%Y")])
            
            if len(batch) >= BATCH_SIZE:
                write_batch(client, batch)
                batch = []
                time.sleep(random.uniform(3, 5))
            
            time.sleep(random.uniform(5, 8))
        
        if batch:
            write_batch(client, batch)
            
        print(f"\n🎉 DONE: {len(symbols)} symbols!")
        
    except Exception as e:
        print(f"💥 {e}")
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    main()
