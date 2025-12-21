const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const { JWT } = require('google-auth-library');
const { GoogleSpreadsheet } = require('google-spreadsheet');
const fs = require('fs');

puppeteer.use(StealthPlugin());

const STOCK_LIST_ID = '1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4';
const NEW_MV2_ID    = '1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc';
const START_INDEX = parseInt(process.env.START_INDEX || "0");
const END_INDEX   = parseInt(process.env.END_INDEX || "2500");
const CHECKPOINT_FILE = "checkpoint.txt";

const delay = (ms) => new Promise(res => setTimeout(res, ms));

async function run() {
    let lastI = START_INDEX;
    if (fs.existsSync(CHECKPOINT_FILE)) {
        lastI = parseInt(fs.readFileSync(CHECKPOINT_FILE, 'utf8')) || START_INDEX;
    }

    // AUTH HANDLING
    let creds;
    if (process.env.GSPREAD_CREDENTIALS) {
        creds = JSON.parse(process.env.GSPREAD_CREDENTIALS);
    } else {
        creds = require('./credentials.json');
    }

    const auth = new JWT({
        email: creds.client_email,
        key: creds.private_key,
        scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });

    const sourceDoc = new GoogleSpreadsheet(STOCK_LIST_ID, auth);
    const destDoc = new GoogleSpreadsheet(NEW_MV2_ID, auth);
    await sourceDoc.loadInfo();
    await destDoc.loadInfo();

    const sourceSheet = sourceDoc.sheetsByTitle['Sheet1'];
    const destSheet = destDoc.sheetsByTitle['Sheet5'];
    const rows = await sourceSheet.getRows();
    const currentDate = new Date().toLocaleDateString('en-US');

    const browser = await puppeteer.launch({
        headless: "new",
        args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
    });

    console.log(`Starting from row ${lastI}`);

    for (let i = lastI; i <= END_INDEX && i < rows.length; i++) {
        const page = await browser.newPage();
        try {
            const url = rows[i]._rawData[3];
            const name = rows[i]._rawData[0];

            console.log(`[${i}] Scraping: ${name}`);
            await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
            await delay(5000);

            const data = await page.evaluate(() => {
                const elements = Array.from(document.querySelectorAll(".valueValue-l31H9iuA"));
                return elements.slice(0, 14).map(el => el.innerText.replace('−', '-').trim());
            });

            // Fill N/A if less than 14 found
            while (data.length < 14) data.push("N/A");

            await destSheet.addRow([name, currentDate, ...data]);
            fs.writeFileSync(CHECKPOINT_FILE, (i + 1).toString());
            
        } catch (e) {
            console.error(`Error on ${i}: ${e.message}`);
        } finally {
            await page.close();
        }
        await delay(2000);
    }
    await browser.close();
}

run().catch(console.error);
