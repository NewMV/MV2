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

# ... (Keep your Auth and Config sections same) ...

def scrape_tradingview(url, symbol_name):
    if not url: return ["No URL"] * 14
    
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    # Added a more recent User-Agent to prevent GitHub Runner detection
    opts.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=CHROME_SERVICE, options=opts)
    
    try:
        driver.get(url)
        
        # 1. Wait for the data to actually load (the price element)
        wait = WebDriverWait(driver, 35)
        # We wait for the specific container that holds technical values
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "valueValue-l31H9iuA")))
        
        # 2. Force Render: GitHub runners need extra time for JS to execute
        driver.execute_script("window.scrollTo(0, 500);")
        time.sleep(3) 
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # 3. Targeted extraction: Get all value divs
        all_vals = soup.find_all("div", class_="valueValue-l31H9iuA")
        
        extracted = []
        for el in all_vals:
            txt = el.get_text(strip=True).replace('−', '-').replace('∅', '').replace('+', '')
            
            # CRITICAL: Only keep text that looks like a number, percentage, or volume (K/M/B)
            # This prevents labels like "Neutral" or "Buy" from shifting your columns
            if txt and (any(char.isdigit() for char in txt)):
                if txt not in extracted: # Deduplicate price if it appears twice
                    extracted.append(txt)

        # 4. Consistency Check: Pad to exactly 14 columns
        # If your sheet expects 14 specific technicals, we ensure they align
        final_list = extracted[:14]
        while len(final_list) < 14:
            final_list.append("N/A")
            
        return final_list

    except Exception as e:
        print(f"❌ {symbol_name} Fail: {str(e)[:40]}")
        return ["Timeout/Error"] * 14
    finally:
        driver.quit()

# ... (Rest of your batch loop) ...
