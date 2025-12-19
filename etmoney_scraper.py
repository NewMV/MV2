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
import re
import requests

STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

CHUNK_START = int(os.getenv('CHUNK_START', 0))
CHUNK_END = int(os.getenv('CHUNK_END', 2500))
BATCH_SIZE = 20

SYMBOL_ETMONEY_MAP = {
    '360ONE': '360-one-wam-ltd/1035', '3IINFOLTD': '3i-infotech-ltd/1003', 
    '3MINDIA': '3m-india-ltd/1004', '5PAISA': '5paisa-capital-ltd/1005',
    '63MOONS': '63-moons-technologies-ltd/1006', 'A2ZINFRA': 'a2z-infra-engineering-ltd/1007',
}

# 🎯 YOUR EXACT SELECTORS
EXACT_SECTOR_SELECTOR = "#page-container > div.bg-white-color.w-full.mt-4.mb-3\\.5 > div > div > div.w-full.col-span-8 > div > div > div > a"
SHORT_SECTOR_SELECTOR = "div.w-full.col-span-8 div > div > div > a"

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new"); opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage"); opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080"); opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def get_nse_sector_api(symbol):
    try:
        url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', 'Accept': 'application/json', 'Referer': 'https://www.nseindia.com/'}
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return data.get('info', {}).get('industry') or data.get('info', {}).get('sector')
    except: pass
    return None

def extract_sector_exact(soup):
    """🎯 Extract from YOUR EXACT CSS selector"""
    
    # 1. YOUR EXACT SELECTOR (priority #1)
    element = soup.select_one(EXACT_SECTOR_SELECTOR)
    if element:
        sector_text = element.get_text(strip=True)
        if sector_text and len(sector_text) > 2:
            return sector_text.strip()
    
    # 2. SHORT VERSION (backup)
    element = soup.select_one(SHORT_SECTOR_SELECTOR)
    if element:
        sector_text = element.get_text(strip=True)
        if sector_text and len(sector_text) > 2:
            return sector_text.strip()
    
    return None

def scrape_sector_selenium(driver, symbol):
    """🎯 Selenium + YOUR EXACT SELECTOR (most reliable)"""
    try:
        slug = SYMBOL_ETMONEY_MAP.get(symbol)
        url = f"https://www.etmoney.com/stocks/{slug}" if slug else f"https://www.etmoney.com/stocks/{symbol.lower()}-ltd"
        
        print(f"🌐 Visiting: {url}")
        driver.get(url)
        WebDriverWait(driver, 15).until(lambda d: d.execute_script("return document.readyState") == "complete")
        time.sleep(3)  # Let dynamic content load
        
        # 🎯 YOUR EXACT SELECTOR - Selenium version
        try:
            element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, EXACT_SECTOR_SELECTOR))
            )
            sector = element.text.strip()
            if sector and len(sector) > 2:
                print(f"✅ SELENIUM EXACT: '{sector}'")
                return sector
        except:
            print(f"❌ Exact selector failed for {symbol}")
        
        # Fallback: Short selector
        try:
            element = driver.find_element(By.CSS_SELECTOR, SHORT_SECTOR_SELECTOR)
            sector = element.text.strip()
            if sector and len(sector) > 2:
                print(f"✅ SHORT SELECTOR: '{sector}'")
                return sector
        except: pass
        
        # Fallback: BeautifulSoup
        soup = BeautifulSoup(driver.page_source, "html.parser")
        sector = extract_sector_exact(soup)
        if sector:
            print(f"✅ BS EXACT: '{sector}'")
            return sector
            
    except Exception as e:
        print(f"❌ Selenium error {symbol}: {e}")
    
    return None

def get_sector(symbol, driver):
    """Main sector getter - NSE → ET Money exact selector"""
    print(f"🔍 {symbol}")
    
    # 1. Fast NSE API first
    sector = get_nse_sector_api(symbol)
    if sector:
        print(f"✅ NSE API: '{sector}'")
        return sector
    
    # 2. ET Money with YOUR exact selector
    sector = scrape_sector_selenium(driver, symbol)
    return sector or "NO_DATA"

def write_to_sheet6_ordered(client, results, chunk_start, local_index):
    """🎯 WRITE TO EXACT ROW POSITIONS"""
    try:
        sheet = client.open_by_url(NEW_MV2_URL).worksheet("Sheet6")
        start_row = chunk_start + local_index + 2  # +2 for header
        end_row = start_row + len(results) - 1
        range_name = f"A{start_row}:C{end_row}"
        sheet.update(range_name, results)
        print(f"✅ Rows {start_row}-{end_row} WRITTEN ({len(results)} rows)")
        return True
    except Exception as e:
        print(f"❌ Write failed: {e}")
        return False

def main():
    driver = client = None
    print(f"🚀 ET Money EXACT Selector Scraper - Chunk {CHUNK_START}-{CHUNK_END}")
    
    try:
        # Google Sheets auth
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds_json)) if creds_json else gspread.service_account(filename="credentials.json")
        
        # Read symbols
        source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
        all_data = source_sheet.get_all_values()
        all_symbols = [row[0].strip().upper() for row in all_data[1:] if row and row[0].strip()]
        symbols = all_symbols[CHUNK_START:CHUNK_END]
        print(f"📖 Processing {len(symbols)} symbols: {symbols[0]} → {symbols[-1]}")
        
        # CSV backup
        chunk_file = f"chunk_{CHUNK_START}_{CHUNK_END}_sectors_exact_{date.today().strftime('%d%m%Y')}.csv"
        with open(chunk_file, 'w', newline='') as f: 
            csv.writer(f).writerow(['SYMBOL', 'SECTOR', 'DATE'])
        
        driver = get_driver()
        results = []
        local_index = 0
        
        for i, symbol in enumerate(symbols, 1):
            print(f"\n[{i:3d}/{len(symbols)}] {symbol}")
            sector = get_sector(symbol, driver)
            results.append([symbol, sector, date.today().strftime("%d/%m/%Y")])
            
            # Batch write every BATCH_SIZE
            if len(results) >= BATCH_SIZE:
                write_to_sheet6_ordered(client, results, CHUNK_START, local_index)
                with open(chunk_file, 'a', newline='') as f: 
                    csv.writer(f).writerows(results)
                local_index += len(results)
                results = []
                time.sleep(random.uniform(2, 4))  # Anti-ban
        
        # Final batch
        if results:
            write_to_sheet6_ordered(client, results, CHUNK_START, local_index)
            with open(chunk_file, 'a', newline='') as f: 
                csv.writer(f).writerows(results)
        
        print(f"\n🎉 COMPLETE! {len(symbols)} symbols → Sheet6 Rows {CHUNK_START+2}-{CHUNK_END+1}")
        print(f"💾 Backup: {chunk_file}")
        
    except Exception as e: 
        print(f"💥 FATAL ERROR: {e}")
    finally: 
        if driver: 
            driver.quit()
        print("👋 Driver closed")

if __name__ == "__main__":
    main()
