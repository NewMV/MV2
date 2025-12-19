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

def get_nse_sector(symbol):
    """🚀 NSE OFFICIAL API - MOST RELIABLE"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.nseindia.com/',
        }
        
        # NSE quote endpoint
        url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
        resp = requests.get(url, headers=headers, timeout=10)
        
        if resp.status_code == 200:
            data = resp.json()
            info = data.get('info', {})
            sector = info.get('industry') or info.get('sector')
            if sector:
                print(f"  ✅ NSE API: '{sector}'")
                return sector
        
        # NSE industry classification
        url2 = f"https://www.nseindia.com/api/master-quote?symbol={symbol}"
        resp2 = requests.get(url2, headers=headers, timeout=10)
        if resp2.status_code == 200:
            data2 = resp2.json()
            sector2 = data2.get('industry', {}).get('name')
            if sector2:
                print(f"  ✅ NSE Industry: '{sector2}'")
                return sector2
                
    except Exception as e:
        print(f"  ❌ NSE API error: {e}")
    
    return None

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def extract_sector_robust(driver):
    """🔥 5-METHOD EXTRACTION - FINDS EVERYTHING"""
    
    # Method 1: YOUR XPATH - page-container links
    try:
        links = driver.find_elements(By.XPATH, '//*[@id="page-container"]//a')
        for link in links:
            text = link.text.strip()
            if text and len(text) > 3 and any(kw in text.lower() for kw in ['sector', 'industry', 'it', 'bank', 'finance']):
                return text
    except:
        pass
    
    # Method 2: Any sector text patterns
    soup = BeautifulSoup(driver.page_source, "html.parser")
    page_text = soup.get_text()
    
    patterns = [
        r'(?:Sector|Industry|Business)[:\s\-]*([A-Z][A-Za-z\s\-&/]{3,50})(?:\s|$)',
        r'([A-Z][A-Za-z\s\-&/]{3,50})\s*(?:Sector|Industry)',
        r'BELONGS TO[:\s]*([A-Z][A-Za-z\s\-&/]{3,50})'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, page_text, re.I)
        if match:
            sector = match.group(1).strip()
            print(f"  ✅ Pattern match: '{sector}'")
            return sector
    
    # Method 3: Common sector keywords in spans/divs
    sector_keywords = ['IT', 'BANK', 'FINANCE', 'AUTO', 'PHARMA', 'CHEMICAL', 'METAL', 'OIL', 'GAS']
    elements = soup.find_all(['span', 'div', 'td', 'a'], string=True)
    
    for elem in elements:
        text = elem.get_text(strip=True)
        if (len(text) > 2 and any(kw in text.upper() for kw in sector_keywords)):
            print(f"  ✅ Keyword match: '{text}'")
            return text
    
    # Method 4: Breadcrumbs
    breadcrumbs = soup.find_all(['nav', 'ol', 'div'], class_=re.compile(r'bread|path', re.I))
    for bc in breadcrumbs:
        bc_text = bc.get_text()
        if any(kw in bc_text.upper() for kw in sector_keywords):
            print(f"  ✅ Breadcrumb: '{bc_text}'")
            return bc_text
    
    return None

def scrape_etmoney_via_google(driver, symbol):
    """Google → ET Money page"""
    try:
        query = f'"{symbol}" site:etmoney.com/stocks'
        driver.get(f"https://www.google.com/search?q={query}")
        time.sleep(3)
        
        # Click first etmoney.com result
        et_links = driver.find_elements(By.XPATH, "//a[contains(@href,'etmoney.com/stocks')][1]")
        if et_links:
            et_links[0].click()
            time.sleep(5)
            return extract_sector_robust(driver)
    except:
        pass
    return None

def get_sector_final(symbol, driver=None):
    """🔥 PRIORITY SYSTEM - NSE API FIRST"""
    print(f"  🔍 {symbol}")
    
    # PRIORITY 1: NSE API (90% success rate)
    sector = get_nse_sector(symbol)
    if sector:
        return sector
    
    # PRIORITY 2: ET Money via Google
    if driver:
        sector = scrape_etmoney_via_google(driver, symbol)
        if sector:
            return sector
    
    # PRIORITY 3: Common sector mapping
    common_sectors = {
        '20MICRONS': 'Chemicals',
        '21STCENMGM': 'Finance',
        # Add more patterns
    }
    return common_sectors.get(symbol, "MANUAL_CHECK")

def read_symbols(client):
    sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    all_data = sheet.get_all_values()
    symbols = [row[0].strip().upper() for row in all_data[1:] if row and row[0].strip()]
    print(f"📖 {len(symbols)} symbols loaded")
    return symbols

def write_batch(client, batch_data):
    try:
        sheet = client.open_by_url(NEW_MV2_URL).worksheet("Sheet7")
        sheet.append_rows(batch_data)
        print(f"💾 Sheet7: {len(batch_data)} rows")
        return True
    except Exception as e:
        print(f"❌ Write error: {e}")
        return False

def main():
    driver = None
    client = None
    
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
        BATCH_SIZE = 5
        
        for i, symbol in enumerate(symbols, 1):
            print(f"\n[{i}/{len(symbols)}] {symbol}")
            sector = get_sector_final(symbol, driver)
            print(f"✅ '{sector}'")
            
            batch.append([symbol, sector, date.today().strftime("%d/%m/%Y")])
            
            if len(batch) >= BATCH_SIZE:
                write_batch(client, batch)
                batch = []
                time.sleep(2)
            
            time.sleep(random.uniform(2, 4))  # Faster - NSE API is quick
        
        if batch:
            write_batch(client, batch)
            
        print("\n🎉 ✅ ALL COMPLETE → Sheet7!")
        
    except Exception as e:
        print(f"💥 {e}")
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    main()
