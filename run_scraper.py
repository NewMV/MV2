name: 🚀 TradingView 25x Parallel + BATCH READ (10 mins)
on: workflow_dispatch

jobs:
  ultra-fast-scrape:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        shard: [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24]
    timeout-minutes: 60
    
    steps:
    - uses: actions/checkout@v4
    
    - uses: actions/setup-python@v5
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: pip install selenium gspread beautifulsoup4 webdriver-manager
    
    - name: BATCH READ + Calculate shard range
      env:
        GSPREAD_CREDENTIALS: ${{ secrets.GSPREAD_CREDENTIALS }}
      run: |
        echo "$GSPREAD_CREDENTIALS" > credentials.json
        
        # BATCH READ ONCE - All 2500 rows upfront
        python -c "
import gspread, json, os
client = gspread.service_account('credentials.json')
sheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4').worksheet('Sheet1')
data = sheet.get_all_values()[1:]  # Skip header
print(f'✅ Read {len(data)} rows from Sheet1')
with open('data_rows.json', 'w') as f:
    json.dump(data, f)
"
        
        # Calculate shard range (bash math)
        SHARD_INDEX=${{ matrix.shard }}
        START_INDEX=$((SHARD_INDEX * 100))
        END_INDEX=$((START_INDEX + 99))
        echo "Shard ${{ matrix.shard }}: Rows $((START_INDEX+2))-$((END_INDEX+2)) (indices $START_INDEX-$END_INDEX)"
        echo "START_INDEX=$START_INDEX" >> $GITHUB_ENV
        echo "END_INDEX=$END_INDEX" >> $GITHUB_ENV
        echo "CHECKPOINT_FILE=checkpoint_shard_${{ matrix.shard }}.txt" >> $GITHUB_ENV
    
    - name: Setup cookies
      if: env.TRADINGVIEW_COOKIES != ''
      env:
        TRADINGVIEW_COOKIES: ${{ secrets.TRADINGVIEW_COOKIES }}
      run: echo "$TRADINGVIEW_COOKIES" > cookies.json
    
    - name: Run scraper (BATCH data pre-loaded)
      env:
        GSPREAD_CREDENTIALS: ${{ secrets.GSPREAD_CREDENTIALS }}
        TRADINGVIEW_COOKIES: ${{ secrets.TRADINGVIEW_COOKIES }}
        SHARD_INDEX: ${{ matrix.shard }}
        SHARD_STEP: 25
      run: python run_scraper.py
    
    - name: Upload checkpoint
      uses: actions/upload-artifact@v4
      with:
        name: checkpoint-shard-${{ matrix.shard }}
        path: checkpoint_shard_${{ matrix.shard }}.txt
