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

def scrape_sector_etmoney(driver, symbol):
    try:
        print(f"  🔍 Searching: {symbol}")
        driver.get("https://www.etmoney.com/stocks")
        wait = WebDriverWait(driver, 10)
        
        # Multiple search box selectors
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
            
        search_box.clear()
        search_box.send_keys(symbol)
        time.sleep(2)
        search_box.send_keys(Keys.ENTER)
        time.sleep(4)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        sector = extract_sector(soup)
        return sector or "NO_SECTOR"
        
    except Exception as e:
        return f"ERROR: {str(e)[:30]}"

def extract_sector(soup):
    # Method 1: Direct patterns
    patterns = [r'Sector[:\s]*([A-Z][A-Za-z\s\-&/]+?)(?:\s|$)', r'Industry[:\s]*([A-Z][A-Za-z\s\-&/]+?)(?:\s|$)']
    all_text = soup.get_text()
    for pattern in patterns:
        match = re.search(pattern, all_text, re.I)
        if match:
            return match.group(1).strip()
    
    # Method 2: Tables
    for table in soup.find_all('table'):
        for row in table.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                text = ' '.join(c.get_text(strip=True) for c in cells)
                if 'sector' in text.lower():
                    for cell in cells[1:]:
                        sector = cell.get_text(strip=True)
                        if len(sector) > 2:
                            return sector.strip()
    return None

def read_symbols(client):
    sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    all_data = sheet.get_all_values()
    symbols = [row[0].strip().upper() for row in all_data[1:] if row and row[0].strip()]
    print(f"📖 Read {len(symbols)} symbols")
    return symbols

def write_batch(client, batch_data):
    """Write batch to Sheet6 - APPEND MODE"""
    try:
        sheet = client.open_by_url(NEW_MV2_URL).worksheet("Sheet6")
        sheet.append_rows(batch_data)
        print(f"💾 Wrote {len(batch_data)} rows")
        return True
    except Exception as e:
        print(f"❌ Batch write failed: {e}")
        return False

def main():
    driver = None
    client = None
    BATCH_SIZE = 5
    
    try:
        # Auth - ENV first, then file
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        if creds_json:
            client = gspread.service_account_from_dict(json.loads(creds_json))
            print("🔐 ENV auth")
        elif os.path.exists("credentials.json"):
            client = gspread.service_account(filename="credentials.json")
            print("🔐 File auth")
        else:
            raise Exception("No credentials!")
        
        # Read symbols
        symbols = read_symbols(client)
        if not symbols:
            return
        
        # Scrape
        driver = get_driver()
        batch = []
        
        for i, symbol in enumerate(symbols, 1):
            print(f"\n[{i}/{len(symbols)}] {symbol}")
            sector = scrape_sector_etmoney(driver, symbol)
            print(f"   ✅ {sector}")
            
            batch.append([symbol, sector, date.today().strftime("%d/%m/%Y")])
            
            # Write batch when full
            if len(batch) >= BATCH_SIZE:
                write_batch(client, batch)
                batch = []
                time.sleep(random.uniform(3, 5))
            
            time.sleep(random.uniform(4, 7))
        
        # Final batch
        if batch:
            write_batch(client, batch)
        
        print(f"\n🎉 COMPLETED: {len(symbols)} symbols scraped!")
        
    except Exception as e:
        print(f"💥 Error: {e}")
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    main()
