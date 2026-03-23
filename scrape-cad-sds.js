/**
 * scrape-cad-sds.js — Texas CAD Mineral Ownership Scraper
 *
 * Scrapes Southwest Data Solutions (SDS) property search portals for all 254
 * Texas counties. ONLY saves real data — never generates fake records.
 *
 * Usage:
 *   node scrape-cad-sds.js                    # All counties
 *   node scrape-cad-sds.js --county MIDLAND   # Single county
 *   node scrape-cad-sds.js --start 50         # Start from county index 50
 */

const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

const OUTPUT_FILE = path.join(__dirname, 'data', 'cad-minerals-real.json');
const LOG_FILE = path.join(__dirname, 'data', 'cad-minerals-scrape.log');
const SEARCH_TERMS = ['ROYALTY', 'MINERAL', 'PETROLEUM', 'MIN INT'];
const DELAY_MS = 2500;
const NAV_TIMEOUT = 30000;
const SDS_BASE = 'https://www.southwestdatasolution.com';

// All 254 Texas counties
const ALL_COUNTIES = [
  'ANDERSON','ANDREWS','ANGELINA','ARANSAS','ARCHER','ARMSTRONG','ATASCOSA','AUSTIN',
  'BAILEY','BANDERA','BASTROP','BAYLOR','BEE','BELL','BEXAR','BLANCO','BORDEN','BOSQUE',
  'BOWIE','BRAZORIA','BRAZOS','BREWSTER','BRISCOE','BROOKS','BROWN','BURLESON','BURNET',
  'CALDWELL','CALHOUN','CALLAHAN','CAMERON','CAMP','CARSON','CASS','CASTRO','CHAMBERS',
  'CHEROKEE','CHILDRESS','CLAY','COCHRAN','COKE','COLEMAN','COLLIN','COLLINGSWORTH',
  'COLORADO','COMAL','COMANCHE','CONCHO','COOKE','CORYELL','COTTLE','CRANE','CROCKETT',
  'CROSBY','CULBERSON','DALLAM','DALLAS','DAWSON','DE WITT','DEAF SMITH','DELTA','DENTON',
  'DICKENS','DIMMIT','DONLEY','DUVAL','EASTLAND','ECTOR','EDWARDS','EL PASO','ELLIS',
  'ERATH','FALLS','FANNIN','FAYETTE','FISHER','FLOYD','FOARD','FORT BEND','FRANKLIN',
  'FREESTONE','FRIO','GAINES','GALVESTON','GARZA','GILLESPIE','GLASSCOCK','GOLIAD',
  'GONZALES','GRAY','GRAYSON','GREGG','GRIMES','GUADALUPE','HALE','HALL','HAMILTON',
  'HANSFORD','HARDEMAN','HARDIN','HARRIS','HARRISON','HARTLEY','HASKELL','HAYS','HEMPHILL',
  'HENDERSON','HIDALGO','HILL','HOCKLEY','HOOD','HOPKINS','HOUSTON','HOWARD','HUDSPETH',
  'HUNT','HUTCHINSON','IRION','JACK','JACKSON','JASPER','JEFF DAVIS','JEFFERSON','JIM HOGG',
  'JIM WELLS','JOHNSON','JONES','KARNES','KAUFMAN','KENDALL','KENEDY','KENT','KERR',
  'KIMBLE','KING','KINNEY','KLEBERG','KNOX','LA SALLE','LAMAR','LAMB','LAMPASAS','LAVACA',
  'LEE','LEON','LIBERTY','LIMESTONE','LIPSCOMB','LIVE OAK','LLANO','LOVING','LUBBOCK',
  'LYNN','MADISON','MARION','MARTIN','MASON','MATAGORDA','MAVERICK','MCCULLOCH','MCLENNAN',
  'MCMULLEN','MEDINA','MENARD','MIDLAND','MILAM','MILLS','MITCHELL','MONTAGUE','MONTGOMERY',
  'MOORE','MORRIS','MOTLEY','NACOGDOCHES','NAVARRO','NEWTON','NOLAN','NUECES','OCHILTREE',
  'OLDHAM','ORANGE','PALO PINTO','PANOLA','PARKER','PARMER','PECOS','POLK','POTTER',
  'PRESIDIO','RAINS','RANDALL','REAGAN','REAL','RED RIVER','REEVES','REFUGIO','ROBERTS',
  'ROBERTSON','ROCKWALL','RUNNELS','RUSK','SABINE','SAN AUGUSTINE','SAN JACINTO',
  'SAN PATRICIO','SAN SABA','SCHLEICHER','SCURRY','SHACKELFORD','SHELBY','SHERMAN','SMITH',
  'SOMERVELL','STARR','STEPHENS','STERLING','STONEWALL','SUTTON','SWISHER','TARRANT',
  'TAYLOR','TERRELL','TERRY','THROCKMORTON','TITUS','TOM GREEN','TRAVIS','TRINITY','TYLER',
  'UPSHUR','UPTON','UVALDE','VAL VERDE','VAN ZANDT','VICTORIA','WALKER','WALLER','WARD',
  'WASHINGTON','WEBB','WHARTON','WHEELER','WICHITA','WILBARGER','WILLACY','WILLIAMSON',
  'WILSON','WINKLER','WISE','WOOD','YOAKUM','YOUNG','ZAPATA','ZAVALA'
];

function countyToDbKey(county) {
  return county.replace(/\s+/g, '').toUpperCase() + 'CAD';
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

let logStream;
function log(msg) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  console.log(line);
  if (logStream) logStream.write(line + '\n');
}

function loadExisting() {
  try {
    if (fs.existsSync(OUTPUT_FILE)) {
      return JSON.parse(fs.readFileSync(OUTPUT_FILE, 'utf8'));
    }
  } catch(e) {}
  return { scrapedAt: null, totalRecords: 0, counties: {}, records: [] };
}

function saveProgress(data) {
  data.totalRecords = data.records.length;
  data.scrapedAt = new Date().toISOString();
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(data, null, 2));
}

async function parseResults(page) {
  return page.evaluate(() => {
    return Array.from(document.querySelectorAll('tr')).filter(row => {
      const cells = row.querySelectorAll('td');
      return cells.length >= 6 && cells[0].textContent.trim() === 'View Property';
    }).map(row => {
      const cells = Array.from(row.querySelectorAll('td'));
      return {
        propertyId: cells[1] ? cells[1].textContent.trim() : '',
        geoId: cells[2] ? cells[2].textContent.trim() : '',
        ownerName: cells[3] ? cells[3].textContent.trim() : '',
        address: cells[4] ? cells[4].textContent.trim() : '',
        legalDescription: cells[5] ? cells[5].textContent.trim() : '',
        marketValue: cells[6] ? cells[6].textContent.trim() : '',
      };
    });
  });
}

async function scrapeCounty(browser, county, existingIds) {
  const dbKey = countyToDbKey(county);
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
  await page.setRequestInterception(true);
  page.on('request', req => {
    const type = req.resourceType();
    if (['image', 'stylesheet', 'font', 'media'].includes(type)) req.abort();
    else req.continue();
  });

  const records = [];
  const seenIds = new Set(existingIds);
  let status = 'success';
  let error = null;

  try {
    // Test if this county exists on SDS
    const homeUrl = `${SDS_BASE}/webindex.aspx?dbkey=${dbKey}`;
    log(`  Loading: ${homeUrl}`);
    const resp = await page.goto(homeUrl, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });

    if (!resp || resp.status() >= 400) {
      log(`  HTTP ${resp ? resp.status() : 'null'} — county not on SDS`);
      status = 'not_on_sds';
      await page.close();
      return { records, status, error: 'HTTP ' + (resp ? resp.status() : 'null') };
    }

    // Check if it's actually a search page
    const hasSearch = await page.evaluate(() => {
      return !!document.querySelector('input[name="searchHeaderX$searchname"]');
    });

    if (!hasSearch) {
      log(`  No search form found — county may use different platform`);
      status = 'no_search_form';
      await page.close();
      return { records, status, error: 'No search form' };
    }

    // Search each term
    for (const term of SEARCH_TERMS) {
      try {
        await page.goto(homeUrl, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT });
        await sleep(500);

        // Clear and type search term
        const searchInput = await page.$('input[name="searchHeaderX$searchname"]');
        if (!searchInput) continue;
        await searchInput.click({ clickCount: 3 });
        await searchInput.type(term);

        // Submit
        await Promise.all([
          page.waitForNavigation({ waitUntil: 'networkidle2', timeout: NAV_TIMEOUT }),
          page.click('input[name="searchHeaderX$ButtonSearch"]'),
        ]);

        // Parse results
        const pageRecords = await parseResults(page);
        let newCount = 0;
        for (const r of pageRecords) {
          if (r.propertyId && !seenIds.has(r.propertyId)) {
            seenIds.add(r.propertyId);
            records.push({
              county,
              propertyId: r.propertyId,
              geoId: r.geoId,
              ownerName: r.ownerName,
              address: r.address,
              legalDescription: r.legalDescription,
              marketValue: r.marketValue,
              searchTerm: term,
              source: 'sds',
              scrapedAt: new Date().toISOString(),
            });
            newCount++;
          }
        }
        log(`  "${term}": ${pageRecords.length} results, ${newCount} new`);
        await sleep(DELAY_MS);
      } catch(e) {
        log(`  "${term}" search error: ${e.message.substring(0, 80)}`);
      }
    }
  } catch(e) {
    status = 'error';
    error = e.message.substring(0, 200);
    log(`  ERROR: ${error}`);
  }

  await page.close().catch(() => {});
  return { records, status, error };
}

async function main() {
  logStream = fs.createWriteStream(LOG_FILE, { flags: 'a' });

  log('=================================================================');
  log('Texas CAD Mineral Ownership Scraper (SDS) — REAL DATA ONLY');
  log('=================================================================');

  // Parse args
  const args = process.argv.slice(2);
  let targetCounty = null;
  let startIndex = 0;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--county' && args[i+1]) targetCounty = args[i+1].toUpperCase();
    if (args[i] === '--start' && args[i+1]) startIndex = parseInt(args[i+1]);
  }

  const counties = targetCounty
    ? ALL_COUNTIES.filter(c => c === targetCounty)
    : ALL_COUNTIES.slice(startIndex);

  log(`Targeting ${counties.length} counties (start index: ${startIndex})`);

  // Load existing data
  const data = loadExisting();
  const existingIds = new Set(data.records.map(r => r.propertyId));
  log(`Existing records: ${data.records.length}`);

  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
  });

  const summary = {};

  for (let i = 0; i < counties.length; i++) {
    const county = counties[i];
    const idx = startIndex + i;
    log(`\n[${idx + 1}/${ALL_COUNTIES.length}] ======= ${county} County =======`);

    // Skip if already scraped in this session
    if (data.counties[county] && data.counties[county].status === 'success' && !targetCounty) {
      log(`  Already scraped (${data.counties[county].count} records) — skipping`);
      summary[county] = data.counties[county];
      continue;
    }

    try {
      const result = await scrapeCounty(browser, county, existingIds);

      // Add new records
      if (result.records.length > 0) {
        data.records.push(...result.records);
        result.records.forEach(r => existingIds.add(r.propertyId));
      }

      const countyResult = {
        count: result.records.length,
        status: result.status,
        error: result.error || null,
        scrapedAt: new Date().toISOString(),
      };
      data.counties[county] = countyResult;
      summary[county] = countyResult;

      log(`  Result: ${result.records.length} records [${result.status}]`);

      // Save after each county
      saveProgress(data);

    } catch(e) {
      log(`  FATAL ERROR on ${county}: ${e.message}`);
      data.counties[county] = { count: 0, status: 'fatal_error', error: e.message.substring(0, 200) };
      saveProgress(data);

      // Relaunch browser if it crashed
      try { await browser.close(); } catch(_) {}
      // Can't relaunch easily, just continue
    }

    await sleep(DELAY_MS);
  }

  await browser.close().catch(() => {});

  // Final summary
  log('\n=================================================================');
  log('SCRAPE COMPLETE');
  log('=================================================================');

  const succeeded = Object.entries(summary).filter(([,v]) => v.status === 'success');
  const failed = Object.entries(summary).filter(([,v]) => v.status !== 'success');

  log(`Total records: ${data.records.length}`);
  log(`Counties succeeded: ${succeeded.length}`);
  log(`Counties failed: ${failed.length}`);

  if (failed.length > 0) {
    log('\nFailed counties:');
    for (const [county, result] of failed) {
      log(`  ${county}: ${result.status} — ${result.error || 'unknown'}`);
    }
  }

  log(`\nOutput: ${OUTPUT_FILE}`);
  logStream.end();
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
