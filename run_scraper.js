import fs from "fs";
import dotenv from "dotenv";
import puppeteer from "puppeteer-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import { GoogleSpreadsheet } from "google-spreadsheet";
import { JWT } from "google-auth-library";

dotenv.config();
puppeteer.use(StealthPlugin());

// -------- CONFIG -------- //
const STOCK_LIST_SHEET_ID = "1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4";
const NEW_MV2_SHEET_ID = "1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc";

const START_INDEX = Number(process.env.START_INDEX || 0);
const END_INDEX = Number(process.env.END_INDEX || 2500);
const CHECKPOINT_FILE = "checkpoint.txt";

// -------- CHECKPOINT -------- //
let lastIndex = START_INDEX;
if (fs.existsSync(CHECKPOINT_FILE)) {
  lastIndex = Number(fs.readFileSync(CHECKPOINT_FILE, "utf8")) || START_INDEX;
}

console.log(`🔧 Range ${START_INDEX}-${END_INDEX} | Resume ${lastIndex}`);

// -------- GOOGLE AUTH -------- //
const creds = JSON.parse(process.env.GSPREAD_CREDENTIALS);

const auth = new JWT({
  email: creds.client_email,
  key: creds.private_key,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"]
});

const srcDoc = new GoogleSpreadsheet(STOCK_LIST_SHEET_ID, auth);
const dstDoc = new GoogleSpreadsheet(NEW_MV2_SHEET_ID, auth);

await srcDoc.loadInfo();
await dstDoc.loadInfo();

const sourceSheet = srcDoc.sheetsByIndex[0];
const destSheet = dstDoc.sheetsByIndex[4];

const rows = await sourceSheet.getRows();
console.log("✅ Google Sheets connected");

// -------- BROWSER -------- //
const browser = await puppeteer.launch({
  headless: "new",
  args: [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-blink-features=AutomationControlled"
  ]
});

// -------- SCRAPER -------- //
async function scrapeTradingView(url) {
  if (!url) return [];

  const page = await browser.newPage();
  try {
    await page.goto(url, { waitUntil: "networkidle2", timeout: 60000 });
    await page.waitForTimeout(5000);

    const values = await page.$$eval(
      "div[class*='valueValue']",
      els => [...new Set(els.map(e => e.innerText.trim()).filter(Boolean))]
    );

    return values;
  } catch {
    return [];
  } finally {
    await page.close();
  }
}

// -------- MAIN LOOP -------- //
let buffer = [];

for (let i = lastIndex; i < rows.length && i <= END_INDEX; i++) {
  const name = rows[i].get("Name");
  const url = rows[i].get("URL");

  console.log(`🔎 ${i}: ${name}`);
  const values = await scrapeTradingView(url);

  buffer.push({
    Name: name,
    Date: new Date().toISOString().slice(0, 10),
    Values: values.join(", ")
  });

  if (buffer.length >= 5) {
    await destSheet.addRows(buffer);
    buffer = [];
  }

  fs.writeFileSync(CHECKPOINT_FILE, String(i + 1));
  await new Promise(r => setTimeout(r, 2000));
}

if (buffer.length) {
  await destSheet.addRows(buffer);
}

await browser.close();
console.log("🏁 DONE");
