import os, time, json, gspread, random, csv
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

# Chunk config from GitHub Actions
CHUNK_START = int(os.getenv('CHUNK_START', 0))
CHUNK_END = int(os.getenv('CHUNK_END', 2500))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 50))

# NSE → ET Money URL mapping
SYMBOL_ETMONEY_MAP = {
    '360ONE': '360-one-wam-ltd/1035',
    '3IINFOLTD': '3i-infotech-ltd/1003', 
    '3MINDIA': '3m-india-ltd/1004',
    '5PAISA': '5paisa-capital-ltd/1005',
    '63MOONS': '63-moons-technologies-ltd/1006',
    'A2ZINFRA': 'a2z-infra-engineering-ltd/1007',
}

def get_driver():
    """Ultra-stable Chrome for GitHub Actions"""
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    opts.add_argument("--disable-web-security")
    opts.add_argument("--allow-running-insecure-content")
    opts.add_argument("--disable-extensions")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def get_nse_sector_api(symbol):
    """FAST NSE API - 80% hit rate [web:20]"""
    try:
        url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.nseindia.com/'
        }
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return data.get('info', {}).get('industry') or data.get('info', {}).get('sector', None)
    except:
        pass
    return None

def scrape_sector_direct(driver, symbol):
    """Direct ET Money URL scraping"""
    try:
        # Try mapped URL first
        slug = SYMBOL_ETMONEY_MAP.get(symbol)
        if slug:
            url = f"https://www.etmoney.com/stocks/{slug}"
            driver.get(url)
            WebDriverWait(driver, 10).until(lambda d: d.execute_script("return document.readyState") == "complete")
            time.sleep(2)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            sector = extract_sector(soup)
            if sector and sector != "NO_DATA":
                return sector
        
        # Fallback generic URL
        generic_url = f"https://www.etmoney.com/stocks/{symbol.lower()}-ltd"
        driver.get(generic_url)
        WebDriverWait(driver, 10).until(lambda d: d.execute_script("return document.readyState") == "complete")
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        return extract_sector(soup)
    except:
        return None

def extract_sector(soup):
    """Robust sector extraction - multiple methods"""
    text = soup.get_text()
    
    # Regex patterns (most reliable)
    patterns = [
        r'Sector[:\s]*([A-Z][A-Za-z\s\-&/]{2,50})[;\.\s]',
        r'Industry[:\s]*([A-Z][A-Za-z\s\-&/]{2,50})[;\.\s]',
        r'belongs to[:\s]*([A-Z][A-Za-z\s\-&/]{2,50})',
        r'Category[:\s]*([A-Z][A-Za-z\s\-&/]{2,50})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            sector = match.group(1).strip()
            if len(sector) > 2 and not sector.isdigit():
                return sector
    
    # Table scanning
    for table in soup.find_all('table')[:10]:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                row_text = ' '.join(c.get_text(strip=True) for c in cells).lower()
                if any(word in row_text for word in ['sector', 'industry', 'category']):
                    for cell in cells[1:]:
                        sector = cell.get_text(strip=True).strip()
                        if len(sector) > 2 and any(c.isalpha() for c in sector) and len(sector) < 100:
                            return sector
    
    # Breadcrumb fallback
    breadcrumbs = soup.find_all(['nav', 'ol', 'div'], class_=re.compile(r'breadcrumb|nav|path', re.I))
    for bc in breadcrumbs:
        text = bc.get_text()
        match = re.search(r'([A-Z][A-Za-z\s\-&/]{2,50})(?:\s|/|>|→|$)', text)
        if match:
            return match.group(1).strip()
    
    return None

def get_sector(symbol, driver):
    """3-Step fallback: NSE API → Direct URL → NO_DATA"""
    print(f"  🔍 {symbol}")
    
    # Step 1: NSE API (fastest)
    sector = get_nse_sector_api(symbol)
    if sector:
        print(f"  ✅ NSE: {sector}")
        return sector
    
    # Step 2: ET Money Direct
    sector = scrape_sector_direct(driver, symbol)
    if sector:
        print(f"  ✅ ET: {sector}")
        return sector
    
    print(f"  ❌ NO_DATA")
    return "NO_DATA"

def read_symbols_chunk(client):
    """Read exact chunk range from Sheet1"""
    try:
        sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
        all_data = sheet.get_all_values()
        symbols = [row[0].strip().upper() for row in all_data[1:] if row and row[0].strip()]
        
        chunk_symbols = symbols[CHUNK_START:CHUNK_END]
        chunk_file = f"chunk_{CHUNK_START}_{CHUNK_END}_sectors_{date.today().strftime('%d%m%Y')}.csv"
        
        print(f"📖 Chunk {CHUNK_START}-{CHUNK_END}: {len(chunk_symbols)} symbols")
        print(f"💾 Output: {chunk_file}")
        return chunk_symbols, chunk_file
    except Exception as e:
        print(f"❌ Read failed: {e}")
        return [], None

def write_csv_batch(results, chunk_file):
    """Write to CSV (no Google Sheets rate limits!)"""
    with open(chunk_file, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(results)
    print(f"💾 Batch → {chunk_file}: {len(results)} rows")
    return True

def main():
    driver = None
    client = None
    
    print(f"🚀 ET Money Sector Scraper")
    print(f"📊 Chunk: {CHUNK_START}-{CHUNK_END} | Batch: {BATCH_SIZE}")
    
    try:
        # Setup Google Sheets auth
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        if creds_json:
            client = gspread.service_account_from_dict(json.loads(creds_json))
        elif os.path.exists("credentials.json"):
            client = gspread.service_account(filename="credentials.json")
        else:
            raise Exception("❌ No credentials found")
        
        print("✅ Google Sheets connected")
        
        # Read chunk
        symbols, chunk_file = read_symbols_chunk(client)
        if not symbols:
            print("❌ No symbols for this chunk")
            return
        
        if not chunk_file:
            print("❌ CSV filename error")
            return
            
        # Write CSV header
        with open(chunk_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['SYMBOL', 'SECTOR', 'DATE'])
        
        # Setup browser
        driver = get_driver()
        print("✅ Chrome ready")
        
        # Scrape chunk
        results = []
        total_symbols = len(symbols)
        
        for i, symbol in enumerate(symbols, 1):
            print(f"\n[{i:3d}/{total_symbols}] {symbol}")
            sector = get_sector(symbol, driver)
            
            results.append([symbol, sector, date.today().strftime("%d/%m/%Y")])
            
            # Batch write every BATCH_SIZE
            if len(results) >= BATCH_SIZE:
                write_csv_batch(results, chunk_file)
                results = []
                time.sleep(random.uniform(2, 4))  # Anti-ban
        
        # Final batch
        if results:
            write_csv_batch(results, chunk_file)
        
        print(f"\n🎉 Chunk {CHUNK_START}-{CHUNK_END} COMPLETE!")
        print(f"📁 {chunk_file}: {total_symbols} symbols processed")
        
    except Exception as e:
        print(f"💥 ERROR: {e}")
    finally:
        if driver:
            driver.quit()
        print("🏁 Driver closed")

if __name__ == "__main__":
    main()
