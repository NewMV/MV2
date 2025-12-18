name: Parallel TradingView Scraper

on:
  workflow_dispatch: # Allows manual start

jobs:
  scrape:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        # Create 10 parallel instances (Shard 0 to 9)
        shard: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

    steps:
      - name: Checkout Code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install Dependencies
        run: pip install selenium webdriver-manager gspread beautifulsoup4

      - name: Run Scraper Shard ${{ matrix.shard }}
        env:
          GOOGLE_CREDENTIALS: ${{ secrets.GOOGLE_CREDENTIALS }} # Set this in GitHub Secrets
          SHARD_INDEX: ${{ matrix.shard }}
          SHARD_STEP: 10 # Total number of shards in matrix
          START_INDEX: 0
          END_INDEX: 2500
        run: python runscraper.py
