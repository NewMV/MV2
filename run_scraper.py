import os, time, json, gspread, random
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

START_INDEX = int(os.getenv("START_INDEX", "1"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = "checkpoint_new_1.txt"

# ---------------- DRIVER SETUP ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    # Stealth Settings
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument(f"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

# ---------------- SCRAPER LOGIC ---------------- #
def scrape_tradingview(driver, url):
    if not url or "tradingview.com" not in url:
        return []
    
    target_class = "valueValue-l31H9iuA"
    
    for attempt in range(2): # Retry twice
        try:
            driver.get(url)
            
            # Wait for elements to appear and CONTAIN A NUMBER
            # This is the fix for the "ErrorErrorError" issue
            wait = WebDriverWait(driver, 20)
            
            def data_is_populated(d):
                elements = d.find_elements(By.CLASS_NAME, target_class)
                if not elements: return False
                # Check if the text of the first element has at least one digit
                return any(char.isdigit() for char in elements[0].text)

            wait.until(data_is_populated)
            
            # Brief pause for full table rendering
            time.sleep(random.uniform(1.5, 3.0)) 

            soup = BeautifulSoup(driver.page_source, "html.parser")
            raw_vals = soup.find_all("div", class_=target_class)
            
            processed = [
                el.get_text().replace('−', '-').replace('∅', '').strip() 
                for el in raw_vals
            ]
            
            if processed: return processed
            
        except Exception as e:
            print(f"  ⚠️ Attempt {attempt+1} failed for {url[-20:]}")
            if attempt == 0:
                driver.refresh() # Refresh on first failure
                time.sleep(2)
            else:
                # Optional: Uncomment the next line to debug visually
                # driver.save_screenshot(f"error_{int(time.time())}.png")
                pass
                
    return []

# ---------------- MAIN EXECUTION ---------------- #
try:
    # 1. Google Sheets Auth
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    client = gspread.service_account_from_dict(json.loads(creds_json)) if creds_json else gspread.service_account(filename="credentials.json")
    
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:] # Skip header
    
    # 2. Checkpoint Logic
    last_i = START_INDEX
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            try: last_i = int(f.read().strip())
            except: pass

    # 3. Start Scraper
    driver = get_driver()
    current_date = date.today().strftime("%m/%d/%Y")
    batch, batch_start = [], None

    print(f"🚀 Starting scrape from Index {last_i}...")

    for i, row in enumerate(data_rows):
        if i < last_i or i > END_INDEX: continue

        name, url = row[0], row[3] if len(row) > 3 else ""
        target_row_num = i + 2
        if batch_start is None: batch_start = target_row_num

        print(f"🔎 [{i}] Processing: {name}...", end="\r")
        
        vals = scrape_tradingview(driver, url)
        
        # If vals is empty, we fill with "No Data" instead of "Error" to track it
        row_data = [name, current_date] + (vals if vals else ["No Data"] * 10)
        batch.append(row_data)

        # Batch Write every 5 records
        if len(batch) >= 5:
            dest_sheet.update(f"A{batch_start}", batch)
            print(f"💾 Saved rows {batch_start} to {target_row_num}           ")
            batch, batch_start = [], None
            
            # Save Checkpoint
            with open(checkpoint_file, "w") as f:
                f.write(str(i + 1))
            
            time.sleep(random.uniform(1, 2)) # Polite delay

    # Final flush for any remaining items
    if batch:
        dest_sheet.update(f"A{batch_start}", batch)

except Exception as e:
    print(f"\n❌ Critical Error: {e}")
finally:
    if 'driver' in locals(): driver.quit()
    print("\n🏁 Process Complete.")
