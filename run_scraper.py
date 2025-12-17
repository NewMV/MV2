import os
import time
import json
import gspread
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import tempfile

print("🚀 MV2 Scraper - ULTRA STABLE")

# CONFIG
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

ACCOUNT_START = int(os.getenv("ACCOUNT_START", "0"))
ACCOUNT_END = int(os.getenv("ACCOUNT_END", "2500"))
BATCH_INDEX = int(os.getenv("BATCH_INDEX", "0"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))

print(f"📊 Range: {ACCOUNT_START}-{ACCOUNT_END}")

# SECRETS
GSPREAD_CREDENTIALS = os.getenv("GSPREAD_CREDENTIALS")
TRADINGVIEW_COOKIES = os.getenv("TRADINGVIEW_COOKIES")

if not GSPREAD_CREDENTIALS:
    print("❌ GSPREAD_CREDENTIALS missing")
    exit(1)

# Temp files
with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
    f.write(GSPREAD_CREDENTIALS)
    creds_path = f.name

cookies_path = None
if TRADINGVIEW_COOKIES:
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        f.write(TRADINGVIEW_COOKIES)
        cookies_path = f.name

# SHEETS
client = gspread.service_account(filename=creds_path)
source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
dest_sheet = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
print("✅ Sheets connected")

all_rows
