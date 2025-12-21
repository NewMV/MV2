import fs from "fs";
import dotenv from "dotenv";
import puppeteer from "puppeteer-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import { GoogleSpreadsheet } from "google-spreadsheet";

dotenv.config();
puppeteer.use(StealthPlugin());

// ---------------- CONFIG ---------------- //
const STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4";
const NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc";

const START_INDEX = parseInt(process.env.START_INDEX || "0");
const END_INDEX   = parseInt(process.env.END_INDEX || "2500");
const CHECKPOINT_FILE = process.env.CHECKPOINT_FILE || "checkpoint.txt";

// ---------------- CHECKPOINT ---------------- //
let last_i = START_INDEX;
if (fs.existsSync(CHECKPOINT_FILE)) {
  try {
    last_i = parseInt(fs.readFileSync(CHECKPOINT_FILE, "utf8"));
  } catch {}
}
console.log(`🔧 Range ${START_INDEX}-${END_INDEX} | Resume ${last_i}`);

// ---------------- GOOGLE SHEETS ---------------- //
const creds = JSON.parse(process.env.GSPREAD_CREDENTIALS);

const srcDoc = new GoogleSpreadsheet(STOCK_LIST_URL);
const dstDoc = new GoogleSpreadsheet(NEW_MV2_URL);

await srcDoc.useServiceAccountAuth(creds);
await dstDoc.useServiceAccountAuth(creds);

await srcDoc.loadInfo();
await dstDoc.loadInfo();

const sourceSheet = srcDoc.sheetsByTitle["Sheet1"];
const destSheet   = dstDoc.sheetsByTitle["Sheet5"];

const rows = await sourceSheet.getRows();
console.log("✅ Google Sheets connected");

// ---------------- BROWSER ---------------- //
const browser = await puppeteer.launch({
  headless: "new",
  args: [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-blink-features=AutomationControlled",
    "--window-size=1920,1080"
  ]
});

// ---------------- SCRAPER ---------------- //
async function scrapeTradingView(url, name) {
  if (!url) return [];

  const page = await browser.newPage();
  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
  );

  try {
    if (fs.existsSync("cookies.json")) {
      const cookies = JSON.parse(fs.readFileSync("cookies.json"));
      await page.goto("https://www.tradingview.com/", { waitUntil: "domcontentloaded" });
      await page.setCookie(...cookies);
      await page.reload({ waitUntil: "networkidle2" });
    }

    await page.goto(url, { waitUntil: "networkidle2", timeout: 60000 });
    await page.waitForTimeout(6000);

    const values = await page.$$eval(
      "div[class*='valueValue']",
      els =>
        els
          .map(e =>
            e.innerText
              .replace("−", "-")
              .replace("∅", "")
              .trim()
          )
          .filter(v => v.length > 0 && v.length < 50)
    );

    const unique = [...new Set(values)];
    console.log(`📊 ${name}: ${unique.length} values`);
    return unique;

  } catch (e) {
    console.log(`❌ ${name}: ${e.message}`);
    return [];
  } finally {
    await page.close();
  }
}

// ---------------- MAIN LOOP ---------------- //
let batch = [];
let batchStart = null;
let processed = 0;
let success = 0;

console.log("🚀 Scraping started");

for (let i = 0; i < rows.length; i++) {
  if (i < last_i || i < START_INDEX || i > END_INDEX) continue;

  const name = rows[i].get("Name");
  const url  = rows[i].get("URL");
  const targetRow = i + 2;

  if (!batchStart) batchStart = targetRow;

  console.log(`🔎 [${i}] ${name}`);

  const values = await scrapeTradingView(url, name);
  if (values.length) success++;

  // ✅ ALL VALUES IN ONE CELL (SAFE)
  batch.push([
    name,
    new Date().toLocaleDateString("en-US"),
    values.join(", ")
  ]);

  processed++;

  if (batch.length >= 5) {
    await destSheet.addRows(
      batch.map(r => ({
        Name: r[0],
        Date: r[1],
        Values: r[2]
      }))
    );
    console.log(`💾 Saved ${batch.length} rows`);
    batch = [];
    batchStart = null;
    await new Promise(r => setTimeout(r, 2000));
  }

  fs.writeFileSync(CHECKPOINT_FILE, String(i + 1));
  await new Promise(r => setTimeout(r, 1800));
}

// Final flush
if (batch.length) {
  await destSheet.addRows(
    batch.map(r => ({
      Name: r[0],
      Date: r[1],
      Values: r[2]
    }))
  );
}

await browser.close();

console.log("🏁 DONE");
console.log(`📊 Processed ${processed} | Success ${success}`);
