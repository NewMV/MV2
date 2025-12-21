import os, time, json, gspread, re
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

# ... (Keep your CONFIG and GOOGLE SHEETS AUTH sections exactly as they are) ...

def scrape_tradingview(url, symbol_name):
    if not url: return ["No URL"] * 14
    
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080") # Ensure desktop layout
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=CHROME_SERVICE, options=opts)
    
    try:
        driver.get(url)
        
        # 1. WAIT: Specifically for the first data value to appear
        wait = WebDriverWait(driver, 30)
        target_class = "valueValue-l31H9iuA"
        wait.until(EC.visibility_of_element_located((By.CLASS_NAME, target_class)))
        
        # 2. ACTIVATE: Scroll slightly to ensure lazy-loaded technicals render
        driver.execute_script("window.scrollTo(0, 400);")
        time.sleep(2) 
        
        # 3. EXTRACT: Grab only visible values
        soup = BeautifulSoup(driver.page_source, "html.parser")
        # Targeting only the specific value divs
        all_elements = soup.find_all("div", class_=target_class)
        
        extracted = []
        for el in all_elements:
            # Get text and clean it
            val = el.get_text(strip=True).replace('−', '-').replace('∅', '')
            
            # FILTER: Ignore labels like "Strong Buy", "Neutral", etc.
            # We only want numeric data, percentages, or "K/M/B" strings
            if val and not any(word in val for word in ["Buy", "Sell", "Neutral"]):
                extracted.append(val)
        
        # 4. DEDUPLICATE: TradingView often repeats the Price in header and body
        # We use a loop to keep order but remove consecutive or exact duplicates
        final_list = []
        for item in extracted:
            if item not in final_list:
                final_list.append(item)
                
        # 5. PADDING: Ensure exactly 14 columns
        final_list = final_list[:14]
        while len(final_list) < 14:
            final_list.append("N/A")
            
        print(f"  ✅ {symbol_name}: Found {len([v for v in final_list if v != 'N/A'])} values")
        return final_list

    except Exception as e:
        print(f"  ⚠️ Error: {str(e)[:50]}")
        return ["Error"] * 14
    finally:
        driver.quit()

# ---------------- MAIN LOOP ---------------- #
# (Keep your existing batch loop, just ensure it calls scrape_tradingview(url, name))
