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

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"  # WRITES TO SHEET7

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

def scrape_exact_sector(driver, symbol):
    """USE YOUR XPATH: //*[@id="page-container"]//a - EXACT TEXT"""
    try:
        print(f"  🔍 Processing: {symbol}")
        
        # Go to ET Money stocks page
        driver.get("https://www.etmoney.com/stocks")
        wait = WebDriverWait(driver, 10)
        
        # Find search box (multiple selectors)
        search_selectors = [
            "//input[@placeholder*='Search']",
            "//input[@placeholder*='stock']",
            "//input[contains(@class,'search')]",
            "//input[@role='combobox']",
            "input[type='search']"
        ]
        
        search_box = None
        for selector in search_selectors:
            try:
                search_box = wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
                print(f"  ✅ Found search box: {selector}")
                break
            except:
                continue
        
        if not search_box:
            print("  ❌ NO SEARCH BOX FOUND")
            return "NO_SEARCH_BOX"
        
        # Search symbol
        search_box.clear()
        search_box.send_keys(symbol)
        time.sleep(2)
        search_box.send_keys(Keys.ENTER)
        time.sleep(5)  # Wait for page load
        
        # ✅ YOUR XPATH: Extract EXACT sector text
        try:
            # Wait for page container
            wait.until(EC.presence_of_element_located((By.ID, "page-container")))
            
            # Get ALL links in page-container → Find sector
            sector_links = driver.find_elements(By.XPATH, '//*[@id="page-container"]//a')
            print(f"  🔗 Found {len(sector_links)} links in page-container")
            
            sector_text = "NO_SECTOR_LINK"
            
            # Scan each link for sector keywords
            for link in sector_links:
                link_text = link.text.strip()
                if link_text and len(link_text) > 2:
                    link_lower = link_text.lower()
                    if any(keyword in link_lower for keyword in ['sector', 'industry', 'it', 'bank', 'finance', 'auto', 'pharma']):
                        sector_text = link_text
                        print(f"  🎯 EXACT SECTOR: '{sector_text}'")
                        return sector_text
            
            # Fallback: Full page text scan
            soup = BeautifulSoup(driver.page_source, "html.parser")
            page_text = soup.get_text()
            patterns = [
                r'Sector[:\s]*([A-Z][A-Za-z\s\-&/]{2,50})',
                r'Industry[:\s]*([A-Z][A-Za-z\s\-&/]{2,50})'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, page_text, re.I)
                if match:
                    print(f"  ✅ Regex fallback: '{match.group(1).strip()}'")
                    return match.group(1).strip()
                    
        except Exception as xpath_error:
            print(f"  ❌ XPath error: {xpath_error}")
            return "XPATH_ERROR"
            
        return sector_text
        
    except Exception as e:
        print(f"  ❌ Error {symbol}: {str(e)[:50]}")
        return f"ERROR:{str(e)[:20]}"

def read_symbols(client):
    """Read Column A from Stock List → Sheet1"""
    sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    all_data = sheet.get_all_values()
    symbols = [row[0].strip().upper() for row in all_data[1:] if row and row[0].strip()]
    print(f"📖 Read {len(symbols)} symbols from Sheet1 Col A")
    return symbols

def write_batch(client, batch_data):
    """Batch write to SHEET7"""
    try:
        sheet = client.open_by_url(NEW_MV2_URL).worksheet("Sheet7")  # ✅ SHEET7
        sheet.append_rows(batch_data)
        print(f"💾 ✅ BATCH WRITTEN TO SHEET7: {len(batch_data)} rows")
        return True
    except Exception as e:
        print(f"❌ Sheet7 write failed: {e}")
        return False

def main():
    driver = None
    client = None
    BATCH_SIZE = 3  # Smaller batches for testing
    
    try:
        # Authentication
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        if creds_json:
            client = gspread.service_account_from_dict(json.loads(creds_json))
            print("🔐 ENV auth ✅")
        elif os.path.exists("credentials.json"):
            client = gspread.service_account(filename="credentials.json")
            print("🔐 File auth ✅")
        else:
            raise Exception("No credentials found!")
        
        # Read symbols
        symbols = read_symbols(client)
        if not symbols:
            print("❌ No symbols in Sheet1 Col A")
            return
        
        print(f"\n🚀 Starting {len(symbols)} symbols → Sheet7")
        driver = get_driver()
        
        batch = []
        for i, symbol in enumerate(symbols, 1):
            print(f"\n[{i}/{len(symbols)}] 🕐 {symbol}")
            sector = scrape_exact_sector(driver, symbol)
            print(f"   ✅ '{sector}'")
            
            batch.append([symbol, sector, date.today().strftime("%d/%m/%Y")])
            
            # Write batch
            if len(batch) >= BATCH_SIZE:
                write_batch(client, batch)
                batch = []
                time.sleep(random.uniform(3, 5))
            
            # Anti-ban delay
            time.sleep(random.uniform(6, 9))
        
        # Final batch
        if batch:
            write_batch(client, batch)
        
        print(f"\n🎉 ✅ COMPLETE! All data → NewMV2 Sheet7")
        
    except KeyboardInterrupt:
        print("\n⏹️ Stopped by user")
    except Exception as e:
        print(f"\n💥 CRASH: {e}")
    finally:
        if driver:
            driver.quit()
        print("🏁 Driver closed")

if __name__ == "__main__":
    main()
