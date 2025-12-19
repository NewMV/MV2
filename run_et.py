import os, time, json, gspread, random
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
import re

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def get_etmoney_stock_url(symbol):
    """Generate ET Money direct URLs"""
    symbol_clean = re.sub(r'[^\w]', '', symbol.upper())
    
    # Common patterns
    patterns = [
        f"https://www.etmoney.com/stocks/{symbol_clean.lower()}-ltd",
        f"https://www.etmoney.com/stocks/{symbol_clean.lower()}",
        f"https://www.etmoney.com/stocks/company/{symbol_clean.lower()}",
        f"https://www.etmoney.com/stocks/{symbol_clean.lower()}-share-price"
    ]
    return patterns

def scrape_via_google_search(driver, symbol):
    """🚀 METHOD 1: Google → ET Money (BYPASS BLOCK)"""
    try:
        print(f"  🌐 Google searching: {symbol}")
        
        # Google search for ET Money stock page
        search_query = f'"{symbol}" site:etmoney.com/stocks'
        driver.get(f"https://www.google.com/search?q={search_query}")
        time.sleep(3)
        
        # Click first ET Money result
        etmoney_links = driver.find_elements(By.XPATH, "//a[contains(@href,'etmoney.com/stocks')][1]")
        if etmoney_links:
            etmoney_links[0].click()
            time.sleep(5)
            return extract_exact_sector(driver)
        
    except Exception as e:
        print(f"  ❌ Google failed: {e}")
    return None

def scrape_direct_urls(driver, symbol):
    """🚀 METHOD 2: Try direct ET Money URLs"""
    urls = get_etmoney_stock_url(symbol)
    
    for url in urls:
        try:
            print(f"  📄 Trying direct: {url}")
            driver.get(url)
            time.sleep(4)
            
            # Check if valid stock page
            if "etmoney.com/stocks" in driver.current_url and "404" not in driver.current_url:
                sector = extract_exact_sector(driver)
                if sector and sector != "NO_SECTOR":
                    return sector
                    
        except:
            continue
    return None

def extract_exact_sector(driver):
    """✅ YOUR XPATH - Extract EXACT sector text"""
    try:
        # Wait for page container
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "page-container")))
        
        # YOUR XPATH: Get ALL links → Find sector
        sector_links = driver.find_elements(By.XPATH, '//*[@id="page-container"]//a')
        print(f"  🔗 Found {len(sector_links)} links")
        
        # Scan for sector keywords
        for link in sector_links:
            link_text = link.text.strip()
            if link_text and len(link_text) > 3:
                link_lower = link_text.lower()
                if any(kw in link_lower for kw in ['sector', 'industry', 'it ', 'bank', 'finance', 'auto', 'pharma', 'software']):
                    print(f"  🎯 EXACT MATCH: '{link_text}'")
                    return link_text
        
        # Fallback: Page source scan
        soup = BeautifulSoup(driver.page_source, "html.parser")
        text = soup.get_text()
        patterns = [r'Sector[:\s]*([A-Z][A-Za-z\s\-&/]{3,50})']
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                return match.group(1).strip()
                
    except Exception as e:
        print(f"  ❌ Extract error: {e}")
    
    return "NO_SECTOR"

def get_sector_final(symbol, driver):
    """🔥 3-METHOD FALLBACK - NO SEARCH BOX NEEDED"""
    print(f"\n🔍 [{symbol}]")
    
    # Method 1: Google search → ET Money
    sector = scrape_via_google_search(driver, symbol)
    if sector:
        return sector
    
    # Method 2: Direct URLs
    sector = scrape_direct_urls(driver, symbol)
    if sector:
        return sector
    
    # Method 3: NSE fallback (basic)
    return "NSE_PENDING"

def read_symbols(client):
    sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    all_data = sheet.get_all_values()
    symbols = [row[0].strip().upper() for row in all_data[1:] if row and row[0].strip()]
    print(f"📖 Loaded {len(symbols)} symbols")
    return symbols[:50]  # Test first 50

def write_batch(client, batch_data):
    """Write to SHEET7"""
    try:
        sheet = client.open_by_url(NEW_MV2_URL).worksheet("Sheet7")
        sheet.append_rows(batch_data)
        print(f"💾 SHEET7: {len(batch_data)} rows")
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
            print("🔐 ENV auth")
        elif os.path.exists("credentials.json"):
            client = gspread.service_account(filename="credentials.json")
            print("🔐 File auth")
        else:
            raise Exception("No credentials!")
        
        symbols = read_symbols(client)
        driver = get_driver()
        
        batch = []
        BATCH_SIZE = 2
        
        for i, symbol in enumerate(symbols, 1):
            print(f"\n--- [{i}/{len(symbols)}] ---")
            sector = get_sector_final(symbol, driver)
            print(f"✅ {symbol} → '{sector}'")
            
            batch.append([symbol, sector, date.today().strftime("%d/%m/%Y")])
            
            if len(batch) >= BATCH_SIZE:
                write_batch(client, batch)
                batch = []
                time.sleep(2)
            
            time.sleep(random.uniform(8, 12))  # Safe delay
        
        if batch:
            write_batch(client, batch)
            
        print("\n🎉 ALL DONE → Sheet7!")
        
    except Exception as e:
        print(f"💥 {e}")
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    main()
