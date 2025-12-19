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

# ---------------- CONFIG - YOUR SHEETS ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"  # ← Stock List (READ Column A)
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"  # ← NewMV2 Sheet6 (WRITE)

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def scrape_sector_etmoney(driver, symbol):
    """Scrape sector from ET Money using search (works for ANY symbol)"""
    try:
        print(f"  🔍 Searching ET Money for {symbol}")
        
        # Go to ET Money stocks page
        driver.get("https://www.etmoney.com/stocks")
        wait = WebDriverWait(driver, 10)
        
        # Find search box (updated selector)
        search_selectors = [
            "//input[@placeholder*='Search']",
            "//input[@placeholder*='stock']", 
            "//input[contains(@class,'search')]",
            "//input[@role='combobox']"
        ]
        
        search_box = None
        for selector in search_selectors:
            try:
                search_box = wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
                break
            except:
                continue
        
        if not search_box:
            return "NO_SEARCH_BOX"
            
        # Type symbol and hit enter
        search_box.clear()
        search_box.send_keys(symbol)
        time.sleep(2)
        search_box.send_keys(Keys.ENTER)
        time.sleep(4)  # Wait for redirect
        
        # Extract sector from stock page
        soup = BeautifulSoup(driver.page_source, "html.parser")
        sector = extract_sector(soup)
        
        return sector if sector else "NO_SECTOR"
        
    except Exception as e:
        print(f"  ❌ Error {symbol}: {str(e)[:60]}")
        return "ERROR"

def extract_sector(soup):
    """Multi-method sector extraction"""
    
    # Method 1: Look for "Sector: XYZ" patterns
    patterns = [
        r'Sector[:\s]*([A-Z][A-Za-z\s\-&/]+?)(?:\s|$)',
        r'Industry[:\s]*([A-Z][A-Za-z\s\-&/]+?)(?:\s|$)'
    ]
    
    all_text = soup.get_text()
    for pattern in patterns:
        match = re.search(pattern, all_text, re.I)
        if match:
            return match.group(1).strip()
    
    # Method 2: Look in tables
    tables = soup.find_all('table')
    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                row_text = ' '.join(c.get_text(strip=True) for c in cells).lower()
                if 'sector' in row_text or 'industry' in row_text:
                    for cell in cells[1:]:  # Skip header
                        text = cell.get_text(strip=True)
                        if len(text) > 2 and any(word in text.upper() for word in ['IT', 'BANK', 'FINANCE', 'AUTO']):
                            return text.strip()
    
    # Method 3: Breadcrumb/URL sector
    breadcrumbs = soup.find_all(['nav', 'ol', 'div'], class_=re.compile(r'breadcrumb|path', re.I))
    for bc in breadcrumbs:
        text = bc.get_text()
        if any(sector in text.upper() for sector in ['IT', 'BANK', 'FINANCE', 'AUTO', 'PHARMA']):
            return text.strip()
    
    return None

def read_symbols_from_stock_list(client):
    """READ Column A from Stock List → Sheet1"""
    try:
        sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
        all_data = sheet.get_all_values()
        
        # Column A (index 0), skip header row, clean empty cells
        symbols = [row[0].strip().upper() for row in all_data[1:] if row and row[0] and row[0].strip()]
        
        print(f"📖 ✅ Read {len(symbols)} symbols from Stock List Sheet1 Column A")
        print(f"📋 First 5: {symbols[:5]}")
        return symbols
    except Exception as e:
        print(f"❌ ❌ Failed to read Stock List: {e}")
        return []

def write_results_to_newmv2(client, results):
    """WRITE [Symbol, Sector, Date] to NewMV2 → Sheet6"""
    try:
        sheet = client.open_by_url(NEW_MV2_URL).worksheet("Sheet6")
        
        # Clear existing data and add header
        sheet.clear()
        sheet.append_row(["Symbol", "Sector", "Scraped_Date"])
        print("🗑️  ✅ Cleared Sheet6 and added header")
        
        # Write results in batches
        batch_size = 10
        for i in range(0, len(results), batch_size):
            batch = results[i:i+batch_size]
            sheet.append_rows(batch)
            print(f"💾 ✅ Wrote batch {i//batch_size + 1} ({len(batch)} rows)")
            time.sleep(1)  # Rate limit
        
        print(f"🎉 ✅ SUCCESS: Wrote {len(results)} rows to NewMV2 Sheet6")
        return True
        
    except Exception as e:
        print(f"❌ ❌ Failed to write to NewMV2 Sheet6: {e}")
        return False

# ---------------- MAIN - READY TO RUN ---------------- #
def main():
    driver = None
    try:
        # 1. Google Sheets Setup
        print("🔐 Connecting to Google Sheets...")
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        if creds_json:
            client = gspread.service_account_from_dict(json.loads(creds_json))
        else:
            client = gspread.service_account(filename="credentials.json")
        print("✅ Google Sheets connected!")
        
        # 2. READ symbols from Stock List Sheet1 Column A
        symbols = read_symbols_from_stock_list(client)
        if not symbols:
            print("❌ No symbols found. Check Stock List Sheet1 Column A")
            return
        
        # 3. Start scraping
        print(f"\n🚀 Starting scrape for {len(symbols)} symbols...")
        driver = get_driver()
        
        results = []
        for i, symbol in enumerate(symbols, 1):
            print(f"\n[{i}/{len(symbols)}] 🕐 Scraping {symbol}...")
            sector = scrape_sector_etmoney(driver, symbol)
            print(f"   ✅ Found: '{sector}'")
            
            results.append([symbol, sector, date.today().strftime("%d/%m/%Y")])
            
            # Human-like delay
            delay = random.uniform(5, 8)
            print(f"   ⏳ Waiting {delay:.1f}s...")
            time.sleep(delay)
        
        # 4. WRITE results to NewMV2 Sheet6
        print("\n📝 Writing results...")
        success = write_results_to_newmv2(client, results)
        
        if success:
            print(f"\n🎊 ALL DONE! {len(results)} symbols scraped → NewMV2 Sheet6")
        else:
            print("❌ Write failed - check credentials/sheet access")
            
    except KeyboardInterrupt:
        print("\n⏹️  Stopped by user")
    except Exception as e:
        print(f"\n💥 CRASH: {e}")
    finally:
        if driver:
            driver.quit()
        print("🏁 Chrome closed. Script finished.")

if __name__ == "__main__":
    main()
