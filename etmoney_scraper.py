import os, time, json, gspread, random, csv
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
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
BATCH_SIZE = 20  # Smaller batches = fewer API errors

SYMBOL_ETMONEY_MAP = {
    '360ONE': '360-one-wam-ltd/1035',
    '3IINFOLTD': '3i-infotech-ltd/1003', 
    '3MINDIA': '3m-india-ltd/1004',
    '5PAISA': '5paisa-capital-ltd/1005',
    '63MOONS': '63-moons-technologies-ltd/1006',
    'A2ZINFRA': 'a2z-infra-engineering-ltd/1007',
}

# ================= YOUR OTHER FUNCTIONS (get_driver, get_nse_sector_api, etc.) =================
# [KEEP ALL EXISTING FUNCTIONS EXACTLY AS THEY ARE - get_driver(), get_nse_sector_api(), etc.]

def test_sheet_access(client):
    """DIAGNOSTIC: Test Sheet6 write access"""
    print("🔍 TESTING SHEET ACCESS...")
    try:
        # Test 1: Can we open NEW_MV2?
        spreadsheet = client.open_by_url(NEW_MV2_URL)
        print(f"✅ NEW_MV2 opened: {spreadsheet.title}")
        
        # Test 2: List all worksheets
        worksheets = spreadsheet.worksheets()
        print(f"📋 Worksheets: {[ws.title for ws in worksheets]}")
        
        # Test 3: Can we access Sheet6?
        sheet6 = spreadsheet.worksheet("Sheet6")
        print(f"✅ Sheet6 found: {sheet6.row_count} rows currently")
        
        # Test 4: Can we WRITE 1 test row?
        test_row = ["TEST_SYMBOL", "TEST_SECTOR", "19/12/2025"]
        sheet6.append_rows([test_row])
        print(f"✅ WRITE SUCCESS! Test row added to Sheet6")
        return True
        
    except Exception as e:
        print(f"❌ SHEET6 ACCESS FAILED: {type(e).__name__}: {str(e)}")
        print("💡 FIX: Share NEW_MV2 with service account → EDITOR access")
        return False

def write_to_sheet6(client, results, chunk_start, chunk_end):
    """Write with FULL error reporting"""
    try:
        spreadsheet = client.open_by_url(NEW_MV2_URL)
        sheet = spreadsheet.worksheet("Sheet6")
        sheet.append_rows(results)
        print(f"✅ SHEET6: {len(results)} rows → Rows {sheet.row_count-len(results)+1} to {sheet.row_count}")
        return True
    except Exception as e:
        print(f"❌ SHEET6 ERROR: {type(e).__name__}: {str(e)}")
        return False

def main():
    driver = None
    client = None
    
    print("="*60)
    print(f"🚀 ET Money Sector Scraper - Chunk {CHUNK_START}-{CHUNK_END}")
    print("="*60)
    
    try:
        # 1. Auth
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        if creds_json:
            client = gspread.service_account_from_dict(json.loads(creds_json))
            print("✅ Auth: GSPREAD_CREDENTIALS")
        elif os.path.exists("credentials.json"):
            client = gspread.service_account(filename="credentials.json")
            print("✅ Auth: credentials.json")
        else:
            raise Exception("No credentials")
        
        # 2. CRITICAL: Test Sheet6 access FIRST
        if not test_sheet_access(client):
            print("❌ ABORTING: Fix Sheet6 permissions first!")
            return
        
        # 3. Read symbols
        sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
        all_data = sheet.get_all_values()
        symbols = [row[0].strip().upper() for row in all_data[1:] if row and row[0].strip()]
        chunk_symbols = symbols[CHUNK_START:CHUNK_END]
        
        print(f"📖 Chunk {CHUNK_START}-{CHUNK_END}: {len(chunk_symbols)} symbols")
        if not chunk_symbols:
            print("❌ No symbols")
            return
        
        chunk_file = f"chunk_{CHUNK_START}_{CHUNK_END}_sectors_{date.today().strftime('%d%m%Y')}.csv"
        
        # 4. CSV Header
        with open(chunk_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['SYMBOL', 'SECTOR', 'DATE'])
        
        # 5. Scrape
        driver = get_driver()
        results = []
        
        for i, symbol in enumerate(chunk_symbols, 1):
            print(f"[{i:3d}/{len(chunk_symbols)}] {symbol}")
            sector = get_sector(symbol, driver)  # Your existing function
            results.append([symbol, sector, date.today().strftime("%d/%m/%Y")])
            
            if len(results) >= BATCH_SIZE:
                # Write Sheet6
                sheet_success = write_to_sheet6(client, results, CHUNK_START, CHUNK_END)
                print(f"📤 Sheet6: {'✅' if sheet_success else '❌'} | CSV: ✅")
                
                # CSV backup
                with open(chunk_file, 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerows(results)
                
                results = []
                time.sleep(random.uniform(2, 4))
        
        # Final batch
        if results:
            write_to_sheet6(client, results, CHUNK_START, CHUNK_END)
            with open(chunk_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(results)
        
        print(f"\n🎉 COMPLETE: {len(chunk_symbols)} symbols → Sheet6 + {chunk_file}")
        
    except Exception as e:
        print(f"💥 FATAL: {type(e).__name__}: {e}")
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    main()
