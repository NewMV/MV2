import os, time, json, gspread, random, csv
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
import re
import requests

# ---------------- CONFIG ---------------- #
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

def scrape_sector_direct(driver, symbol):
    try:
        slug = SYMBOL_ETMONEY_MAP.get(symbol)
        if slug:
            driver.get(f"https://www.etmoney.com/stocks/{slug}")
            WebDriverWait(driver, 10).until(lambda d: d.execute_script("return document.readyState") == "complete")
            time.sleep(2); soup = BeautifulSoup(driver.page_source, "html.parser")
            sector = extract_sector(soup)
            if sector and sector != "NO_DATA": return sector
        
        driver.get(f"https://www.etmoney.com/stocks/{symbol.lower()}-ltd")
        WebDriverWait(driver, 10).until(lambda d: d.execute_script("return document.readyState") == "complete")
        time.sleep(2)
        return extract_sector(BeautifulSoup(driver.page_source, "html.parser"))
    except: return None

def extract_sector(soup):
    text = soup.get_text()
    patterns = [r'Sector[:\s]*([A-Z][A-Za-z\s\-&/]{2,50})[;\.\s]', r'Industry[:\s]*([A-Z][A-Za-z\s\-&/]{2,50})[;\.\s]']
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match: return match.group(1).strip()
    return None

def get_sector(symbol, driver):
    sector = get_nse_sector_api(symbol)
    if sector: return sector
    sector = scrape_sector_direct(driver, symbol)
    return sector or "NO_DATA"

def write_to_sheet6(client, results):
    try:
        sheet = client.open_by_url(NEW_MV2_URL).worksheet("Sheet6")
        sheet.append_rows(results)
        return True
    except: return False

def main():
    driver = client = None
    print(f"🚀 Chunk {CHUNK_START}-{CHUNK_END}")
    
    try:
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds_json)) if creds_json else gspread.service_account(filename="credentials.json")
        
        symbols = [row[0].strip().upper() for row in client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1").get_all_values()[1:] if row and row[0].strip()][CHUNK_START:CHUNK_END]
        chunk_file = f"chunk_{CHUNK_START}_{CHUNK_END}_sectors_{date.today().strftime('%d%m%Y')}.csv"
        
        with open(chunk_file, 'w', newline='') as f: csv.writer(f).writerow(['SYMBOL', 'SECTOR', 'DATE'])
        driver = get_driver()
        
        results = []
        for i, symbol in enumerate(symbols, 1):
            print(f"[{i}/{len(symbols)}] {symbol}")
            results.append([symbol, get_sector(symbol, driver), date.today().strftime("%d/%m/%Y")])
            
            if len(results) >= BATCH_SIZE:
                write_to_sheet6(client, results)
                with open(chunk_file, 'a', newline='') as f: csv.writer(f).writerows(results)
                results = []; time.sleep(random.uniform(2, 4))
        
        if results:
            write_to_sheet6(client, results)
            with open(chunk_file, 'a', newline='') as f: csv.writer(f).writerows(results)
            
        print(f"🎉 {len(symbols)} symbols → Sheet6 + {chunk_file}")
        
    except Exception as e: print(f"💥 {e}")
    finally: driver.quit() if driver else None

if __name__ == "__main__": main()
