#!/usr/bin/env node
/**
 * scrape-fracfocus.js
 * Scrape FracFocus chemical disclosure data for Texas wells.
 * If scraping fails, writes an EMPTY array — NEVER fake data.
 */

const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

const DATA_DIR = path.join(__dirname, 'data');
const OUT_FILE = path.join(DATA_DIR, 'fracfocus.json');

async function attemptScrape() {
  let browser;
  try {
    console.log('Attempting to scrape FracFocus data...');
    browser = await puppeteer.launch({
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox']
    });

    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36');
    page.setDefaultNavigationTimeout(30000);

    await page.goto('https://fracfocus.org/wells/advanced', {
      waitUntil: 'networkidle2',
      timeout: 30000
    });

    const title = await page.title();
    console.log(`Page loaded: ${title}`);
    console.log('FracFocus uses complex React SPA. No data scraped.');
    await browser.close();
    return null;

  } catch (err) {
    console.log(`Scrape failed: ${err.message}`);
    if (browser) await browser.close();
    return null;
  }
}

async function main() {
  console.log('=== FracFocus Chemical Disclosure Scraper ===');

  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

  let data = await attemptScrape();
  if (!data) {
    console.log('No real data available. Writing empty array.');
    data = [];
  }

  const output = {
    source: 'FracFocus Chemical Disclosure Registry',
    records: data,
    count: data.length,
    scraped: new Date().toISOString(),
    ...(data.length === 0 ? { error: 'scraping failed — no real data available' } : {})
  };

  fs.writeFileSync(OUT_FILE, JSON.stringify(output, null, 2));
  console.log(`Saved ${data.length} frac disclosures to ${OUT_FILE}`);
  console.log('Done.');
}

main().catch(console.error);
