#!/usr/bin/env node
/**
 * scrape-cad-minerals.js
 *
 * Scrapes mineral interest data from Texas County Appraisal District websites
 * for the top 15 oil & gas producing counties.
 *
 * If scraping fails for a county, writes EMPTY records — NEVER fake data.
 *
 * Usage:  node scrape-cad-minerals.js
 * Output: data/cad-mineral-ownership.json
 */

const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const OUTPUT_PATH = path.join(__dirname, 'data', 'cad-mineral-ownership.json');
const SCREENSHOT_DIR = path.join(__dirname, 'data', 'raw', 'screenshots');
const DELAY_MS = 3000;
const NAV_TIMEOUT = 30000;

const COUNTIES = [
  { name: 'Midland',  domain: 'midlandcad.org'  },
  { name: 'Ector',    domain: 'ectorcad.org'     },
  { name: 'Reeves',   domain: 'reevescad.org'    },
  { name: 'Andrews',  domain: 'andrewscad.org'   },
  { name: 'Howard',   domain: 'howardcad.org'    },
  { name: 'Martin',   domain: 'martincad.org'    },
  { name: 'Ward',     domain: 'wardcad.org'      },
  { name: 'Pecos',    domain: 'pecoscad.org'     },
  { name: 'Karnes',   domain: 'karnescad.org'    },
  { name: 'DeWitt',   domain: 'dewittcad.org'    },
  { name: 'Webb',     domain: 'webbcad.org'      },
  { name: 'Upton',    domain: 'uptoncad.org'     },
  { name: 'Crane',    domain: 'cranecad.org'     },
  { name: 'Loving',   domain: 'lovingcad.org'    },
  { name: 'Reagan',   domain: 'reagancad.org'    },
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// Scrape helpers — strategies for common CAD website patterns
// ---------------------------------------------------------------------------

async function tryTrueAutomation(page, county, domain) {
  const cadCode = county.toLowerCase() + 'cad';
  const urls = [
    `https://propaccess.trueautomation.com/${cadCode}/search.php`,
    `https://propaccess.trueautomation.com/${cadCode}/`,
    `https://esearch.${domain}/`,
    `https://${domain}/Search`,
    `https://www.${domain}/`,
    `https://${domain}/`,
  ];

  for (const url of urls) {
    try {
      console.log(`    Trying ${url} ...`);
      const resp = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT });
      if (!resp || resp.status() >= 400) continue;
      await sleep(1500);

      const hasTASearch = await page.evaluate(() => {
        const text = document.body?.innerText || '';
        return text.includes('Property Search') || text.includes('Owner Name') ||
               text.includes('prop_id') || text.includes('owner_name');
      });

      if (hasTASearch) {
        console.log(`    Found property search page at ${url}`);
        return { url, type: 'true_automation' };
      }

      const hasSearch = await page.evaluate(() => {
        return document.querySelectorAll('input[type="text"], input[type="search"]').length > 0;
      });

      if (hasSearch) {
        console.log(`    Found search form at ${url}`);
        return { url, type: 'generic_search' };
      }
    } catch (err) {
      // Navigation timeout or other error — try next URL
    }
  }
  return null;
}

async function searchMineralRecords(page, searchInfo) {
  const results = [];

  try {
    if (searchInfo.type === 'true_automation') {
      const searchTerms = ['MINERAL', 'MIN', 'ROYALTY'];

      for (const term of searchTerms) {
        try {
          const ownerInput = await page.$('input[name="owner_name"], input[name="OwnerName"], #owner_name, #OwnerName');
          if (ownerInput) {
            await ownerInput.click({ clickCount: 3 });
            await ownerInput.type(term, { delay: 50 });

            const searchBtn = await page.$('input[type="submit"], button[type="submit"], input[value="Search"], button:has-text("Search")');
            if (searchBtn) {
              await searchBtn.click();
              await sleep(3000);

              const tableData = await page.evaluate(() => {
                const rows = document.querySelectorAll('table tr, .search-result, .result-row');
                const data = [];
                rows.forEach(row => {
                  const cells = row.querySelectorAll('td, .cell');
                  if (cells.length >= 3) {
                    data.push({
                      propertyId: cells[0]?.innerText?.trim() || '',
                      ownerName: cells[1]?.innerText?.trim() || '',
                      legalDescription: cells[2]?.innerText?.trim() || '',
                      assessedValue: cells[3]?.innerText?.trim() || '',
                    });
                  }
                });
                return data;
              });

              if (tableData.length > 0) {
                results.push(...tableData);
              }
            }
          }

          const propInput = await page.$('input[name="prop_id"], input[name="PropertyId"], #prop_id');
          if (propInput && results.length === 0) {
            await page.goto(searchInfo.url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT });
            await sleep(1500);
          }
        } catch (e) {
          // Continue to next search term
        }

        if (results.length > 0) break;
        await sleep(DELAY_MS);
      }

      // If owner search didn't work, try known operator names
      if (results.length === 0) {
        const operators = ['PIONEER', 'DIAMONDBACK', 'EOG'];
        for (const op of operators) {
          try {
            await page.goto(searchInfo.url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT });
            await sleep(1500);

            const ownerInput = await page.$('input[name="owner_name"], input[name="OwnerName"], #owner_name, #OwnerName');
            if (ownerInput) {
              await ownerInput.click({ clickCount: 3 });
              await ownerInput.type(op, { delay: 50 });

              const searchBtn = await page.$('input[type="submit"], button[type="submit"]');
              if (searchBtn) {
                await searchBtn.click();
                await sleep(3000);

                const tableData = await page.evaluate(() => {
                  const rows = document.querySelectorAll('table tr, .search-result');
                  const data = [];
                  rows.forEach(row => {
                    const cells = row.querySelectorAll('td');
                    if (cells.length >= 3) {
                      const text = row.innerText.toLowerCase();
                      if (text.includes('mineral') || text.includes('min ') || text.includes('royalty')) {
                        data.push({
                          propertyId: cells[0]?.innerText?.trim() || '',
                          ownerName: cells[1]?.innerText?.trim() || '',
                          legalDescription: cells[2]?.innerText?.trim() || '',
                          assessedValue: cells[3]?.innerText?.trim() || '',
                        });
                      }
                    }
                  });
                  return data;
                });

                if (tableData.length > 0) {
                  results.push(...tableData);
                }
              }
            }
          } catch (e) {
            // continue
          }
          if (results.length > 10) break;
          await sleep(DELAY_MS);
        }
      }
    }
  } catch (err) {
    console.log(`    Search error: ${err.message}`);
  }

  return results;
}

// ---------------------------------------------------------------------------
// Main scrape routine per county
// ---------------------------------------------------------------------------

async function scrapeCounty(browser, county, domain) {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`  Scraping ${county} CAD — ${domain}`);
  console.log('='.repeat(60));

  const page = await browser.newPage();
  page.setDefaultTimeout(NAV_TIMEOUT);
  await page.setUserAgent(
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 ' +
    '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
  );

  let records = [];

  try {
    const searchInfo = await tryTrueAutomation(page, county, domain);

    if (searchInfo) {
      const screenshotPath = path.join(SCREENSHOT_DIR, `${county.toLowerCase()}-cad-search.png`);
      await page.screenshot({ path: screenshotPath, fullPage: true });
      console.log(`    Screenshot saved: ${screenshotPath}`);

      const scraped = await searchMineralRecords(page, searchInfo);

      if (scraped.length > 0) {
        console.log(`    Scraped ${scraped.length} records from live site`);
        records = scraped.map(r => ({
          county,
          propertyId: r.propertyId,
          ownerName: r.ownerName,
          legalDescription: r.legalDescription,
          assessedValue: parseInt(String(r.assessedValue).replace(/[^0-9]/g, '')) || 0,
          propertyType: 'MINERAL',
          mineralInterestPct: null,
          source: 'scraped',
          scrapeDate: new Date().toISOString().slice(0, 10),
        }));
      } else {
        console.log(`    No mineral records found via scraping. Writing empty for ${county}.`);
      }
    } else {
      console.log(`    Could not find property search page. Writing empty for ${county}.`);
      try {
        const screenshotPath = path.join(SCREENSHOT_DIR, `${county.toLowerCase()}-cad-landing.png`);
        await page.screenshot({ path: screenshotPath, fullPage: true });
        console.log(`    Screenshot saved: ${screenshotPath}`);
      } catch (_) {}
    }
  } catch (err) {
    console.log(`    Error scraping ${county} CAD: ${err.message}`);
  } finally {
    await page.close();
  }

  console.log(`    Total records for ${county}: ${records.length}`);
  return records;
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

(async () => {
  console.log('CAD Mineral Ownership Scraper');
  console.log('Targeting top 15 Texas oil & gas producing counties\n');

  fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  fs.mkdirSync(SCREENSHOT_DIR, { recursive: true });

  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
    ],
  });

  const allRecords = [];
  const summary = [];

  for (const { name, domain } of COUNTIES) {
    try {
      const records = await scrapeCounty(browser, name, domain);
      allRecords.push(...records);
      summary.push({ county: name, count: records.length, source: records.length > 0 ? 'scraped' : 'empty' });
    } catch (err) {
      console.error(`  FATAL error on ${name}: ${err.message}`);
      // No fallback — just record zero for this county
      summary.push({ county: name, count: 0, source: 'error' });
    }

    await sleep(DELAY_MS);
  }

  await browser.close();

  const output = {
    metadata: {
      description: 'Texas CAD Mineral Ownership Data — Top 15 Oil & Gas Counties',
      generatedAt: new Date().toISOString(),
      totalRecords: allRecords.length,
      counties: summary,
    },
    records: allRecords,
  };

  fs.writeFileSync(OUTPUT_PATH, JSON.stringify(output, null, 2));
  console.log(`\n${'='.repeat(60)}`);
  console.log(`Output written to ${OUTPUT_PATH}`);
  console.log(`Total records: ${allRecords.length}`);
  console.log('='.repeat(60));

  console.log('\nCounty Summary:');
  console.log('-'.repeat(45));
  for (const s of summary) {
    console.log(`  ${s.county.padEnd(12)} ${String(s.count).padStart(5)} records  [${s.source}]`);
  }
  console.log('-'.repeat(45));
})();
