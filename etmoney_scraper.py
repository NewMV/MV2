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

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

# ---------------- DRIVER SETUP ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def scrape_et_sector(driver, url):
    if not url or "etmoney.com" not in url:
        return "Invalid URL"
    try:
        driver.get(url)
        # Wait for the main container that usually holds sector info
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(2) # Allow JS to render the labels
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # ET Money specific sector search
        # Strategy: Look for the text "Sector" and get the next element
        sector_text = "N/A"
        elements = soup.find_all(string=lambda t: t and "Sector" in t)
        
        for el in elements:
            parent = el.parent
            # Check for common ET Money patterns (Sector: Technology)
            full_text = parent.get_text(separator=" ").strip()
            if "Sector" in full_text and len(full_text) > 7:
                sector_text = full_text.split("Sector")[-1].replace(":", "").strip()
                break
        
        return sector_text
    except Exception as e:
        return f"Error"

# ---------------- MAIN ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    client = gspread.service_account_from_dict(json.loads(creds_json))
    
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet6")
    
    data_rows = source_sheet.get_all_values()[1:] # Skip header
    driver = get_driver()
    
    results_batch = []
    # Using a small slice for testing, or use your SHARD logic
    for i, row in enumerate(data_rows[:100]): 
        name = row[0]
        # Assuming ET Money URL is in Column E (index 4)
        et_url = row[4] if len(row) > 4 else "" 
        
        print(f"🔎 Processing {name}...")
        sector = scrape_et_sector(driver, et_url)
        
        results_batch.append([name, sector, date.today().strftime("%d/%m/%Y")])
        
        if len(results_batch) >= 10:
            dest_sheet.append_rows(results_batch)
            results_batch = []
            time.sleep(1)

    if results_batch:
        dest_sheet.append_rows(results_batch)

finally:
    if 'driver' in locals(): driver.quit()
