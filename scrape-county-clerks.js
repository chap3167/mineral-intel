#!/usr/bin/env node
/**
 * scrape-county-clerks.js
 * Scrape county clerk records for oil & gas documents.
 * If scraping fails, writes an EMPTY array — NEVER fake data.
 */

const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

const DATA_DIR = path.join(__dirname, 'data');
const OUT_FILE = path.join(DATA_DIR, 'county-clerk-records.json');

async function attemptScrape() {
  let browser;
  try {
    console.log('Attempting to scrape county clerk records...');
    browser = await puppeteer.launch({
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox']
    });

    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36');
    page.setDefaultNavigationTimeout(30000);

    await page.goto('https://www.courthousedirect.com/', {
      waitUntil: 'networkidle2',
      timeout: 30000
    });

    const title = await page.title();
    console.log(`Page loaded: ${title}`);
    console.log('County clerk portals require subscription/authentication. No data scraped.');
    await browser.close();
    return null;

  } catch (err) {
    console.log(`Scrape failed: ${err.message}`);
    if (browser) await browser.close();
    return null;
  }
}

async function main() {
  console.log('=== County Clerk Records Scraper ===');

  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

  let data = await attemptScrape();
  if (!data) {
    console.log('No real data available. Writing empty array.');
    data = [];
  }

  const output = {
    source: 'County Clerk Records',
    records: data,
    count: data.length,
    scraped: new Date().toISOString(),
    ...(data.length === 0 ? { error: 'scraping failed — no real data available' } : {})
  };

  fs.writeFileSync(OUT_FILE, JSON.stringify(output, null, 2));
  console.log(`Saved ${data.length} records to ${OUT_FILE}`);
  console.log('Done.');
}

main().catch(console.error);
