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

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new") # Run in background
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def get_sector_by_search(driver, stock_name):
    try:
        # 1. Go to ET Money Stocks Home
        driver.get("https://www.etmoney.com/stocks")
        wait = WebDriverWait(driver, 10)
        
        # 2. Find Search Box and type Name
        # ET Money search input selector
        search_box = wait.until(EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Search Stocks']")))
        search_box.clear()
        search_box.send_keys(stock_name)
        time.sleep(2) # Wait for dropdown results
        search_box.send_keys(Keys.ENTER) # Hit Enter for first result
        
        # 3. Wait for the Stock Page to load
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(2)
        
        # 4. Extract Sector using BeautifulSoup
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Look for the label "Sector"
        sector_val = "Not Found"
        elements = soup.find_all(string=lambda t: t and "Sector" in t)
        
        for el in elements:
            parent = el.parent
            full_text = parent.get_text(separator=" ").strip()
            if "Sector" in full_text:
                # Cleaning the text (e.g., "Sector : IT - Software" -> "IT - Software")
                sector_val = full_text.split("Sector")[-1].replace(":", "").strip()
                break
        
        return sector_val
    except Exception as e:
        print(f"  ❌ Search failed for {stock_name}: {e}")
        return "N/A"

# ---------------- MAIN ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    client = gspread.service_account_from_dict(json.loads(creds_json)) if creds_json else gspread.service_account(filename="credentials.json")
    
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet6")
    
    data_rows = source_sheet.get_all_values()[1:] # Start from Row 2
    driver = get_driver()
    
    batch = []
    print("🚀 Starting Search-based Scraping...")

    for i, row in enumerate(data_rows):
        stock_name = row[0] # Assumes Name/Symbol is in Column A
        print(f"🔎 Processing [{i}]: {stock_name}")
        
        sector = get_sector_by_search(driver, stock_name)
        batch.append([stock_name, sector, date.today().strftime("%d/%m/%Y")])
        
        # Write to Sheet6 in batches of 5 to avoid API rate limits
        if len(batch) >= 5:
            dest_sheet.append_rows(batch)
            print(f"💾 Saved batch of 5 stocks.")
            batch = []
            time.sleep(random.uniform(2, 4)) # Human-like delay

    if batch:
        dest_sheet.append_rows(batch)

finally:
    if 'driver' in locals(): driver.quit()
    print("🏁 Done.")
