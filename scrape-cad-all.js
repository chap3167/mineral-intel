/**
 * scrape-cad-all.js — Texas CAD Mineral Ownership Scraper (All Platforms)
 *
 * Tries SDS first, then BIS eSearch. ONLY saves REAL data.
 * NEVER generates fake/demo/fallback data.
 *
 * Usage:
 *   node scrape-cad-all.js                    # All 254 counties
 *   node scrape-cad-all.js --county MIDLAND   # Single county
 *   node scrape-cad-all.js --start 50         # Start from county index 50
 */

const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

const OUTPUT_FILE = path.join(__dirname, 'data', 'cad-minerals-real.json');
const LOG_FILE = path.join(__dirname, 'data', 'cad-all-scrape.log');
const SEARCH_TERMS = ['ROYALTY', 'MINERAL', 'PETROLEUM'];
const DELAY_MS = 2500;
const SDS_BASE = 'https://www.southwestdatasolution.com';

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

function cadCode(county) {
  return county.replace(/\s+/g, '').toLowerCase() + 'cad';
}
function dbKey(county) {
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
    if (fs.existsSync(OUTPUT_FILE)) return JSON.parse(fs.readFileSync(OUTPUT_FILE, 'utf8'));
  } catch(e) {}
  return { scrapedAt: null, totalRecords: 0, counties: {}, records: [] };
}

function saveProgress(data) {
  data.totalRecords = data.records.length;
  data.scrapedAt = new Date().toISOString();
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(data, null, 2));
}

// ===================== SDS PLATFORM =====================

async function trySDS(browser, county, existingIds) {
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36');
  await page.setRequestInterception(true);
  page.on('request', req => {
    if (['image','stylesheet','font','media'].includes(req.resourceType())) req.abort();
    else req.continue();
  });

  const records = [];
  const seenIds = new Set(existingIds);
  const key = dbKey(county);

  try {
    const url = `${SDS_BASE}/webindex.aspx?dbkey=${key}`;
    const resp = await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });
    if (!resp || resp.status() >= 400) { await page.close(); return null; }

    const hasForm = await page.evaluate(() => !!document.querySelector('input[name="searchHeaderX$searchname"]'));
    if (!hasForm) { await page.close(); return null; }

    for (const term of SEARCH_TERMS) {
      try {
        await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });
        await sleep(500);
        const input = await page.$('input[name="searchHeaderX$searchname"]');
        if (!input) continue;
        await input.click({ clickCount: 3 });
        await input.type(term);
        await Promise.all([
          page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 30000 }),
          page.click('input[name="searchHeaderX$ButtonSearch"]'),
        ]);

        const pageRecords = await page.evaluate(() => {
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

        let newCount = 0;
        for (const r of pageRecords) {
          if (r.propertyId && !seenIds.has(county + ':' + r.propertyId)) {
            seenIds.add(county + ':' + r.propertyId);
            records.push({ county, ...r, searchTerm: term, source: 'sds', scrapedAt: new Date().toISOString() });
            newCount++;
          }
        }
        log(`    SDS "${term}": ${pageRecords.length} results, ${newCount} new`);
        await sleep(DELAY_MS);
      } catch(e) { log(`    SDS "${term}" error: ${e.message.substring(0, 60)}`); }
    }
  } catch(e) { await page.close().catch(() => {}); return null; }

  await page.close().catch(() => {});
  return records.length > 0 ? records : null;
}

// ===================== BIS ESEARCH PLATFORM =====================

// Custom URL overrides — verified working BIS eSearch and other platforms
const BIS_URL_OVERRIDES = {
  'ANGELINA': 'https://esearch.angelinacad.org/',
  'ARANSAS': 'https://esearch.aransascad.org/',
  'AUSTIN': 'https://esearch.austincad.org/',
  'BELL': 'https://esearch.bellcad.org/',
  'BEXAR': 'https://esearch.bexarcad.org/',
  'BORDEN': 'https://esearch.bordencad.org/',
  'BOWIE': 'https://esearch.bowiecad.org/',
  'BURLESON': 'https://esearch.burlesoncad.org/',
  'CASTRO': 'https://esearch.castrocad.org/',
  'CHEROKEE': 'https://esearch.cherokeecad.org/',
  'COKE': 'https://esearch.cokecad.org/',
  'COLLIN': 'https://esearch.collincad.org/',
  'COLORADO': 'https://esearch.coloradocad.org/',
  'CORYELL': 'https://esearch.coryellcad.org/',
  'DALLAM': 'https://esearch.dallamcad.org/',
  'DALLAS': 'https://esearch.dallascad.org/',
  'DEAF SMITH': 'https://esearch.deafsmithcad.org/',
  'ECTOR': 'https://search.ectorcad.org/',
  'EDWARDS': 'https://esearch.edwardscad.org/',
  'FALLS': 'https://esearch.fallscad.org/',
  'FANNIN': 'https://esearch.fannincad.org/',
  'FORT BEND': 'https://esearch.fortbendcad.org/',
  'GARZA': 'https://esearch.garzacad.org/',
  'GILLESPIE': 'https://esearch.gillespiecad.org/',
  'GRAY': 'https://esearch.graycad.org/',
  'HAMILTON': 'https://esearch.hamiltoncad.org/',
  'HARTLEY': 'https://esearch.hartleycad.org/',
  'HUDSPETH': 'https://esearch.hudspethcad.org/',
  'JACKSON': 'https://esearch.jacksoncad.org/',
  'KERR': 'https://esearch.kerrcad.org/',
  'KIMBLE': 'https://esearch.kimblecad.org/',
  'KINNEY': 'https://esearch.kinneycad.org/',
  'LAMAR': 'https://esearch.lamarcad.org/',
  'LAMB': 'https://esearch.lambcad.org/',
  'MADISON': 'https://esearch.madisoncad.org/',
  'MASON': 'https://esearch.masoncad.org/',
  'MATAGORDA': 'https://esearch.matagordacad.org/',
  'MCMULLEN': 'https://esearch.mcmullencad.org/',
  'MEDINA': 'https://esearch.medinacad.org/',
  'MILLS': 'https://esearch.millscad.org/',
  'MITCHELL': 'https://esearch.mitchellcad.org/',
  'MOORE': 'https://esearch.moorecad.org/',
  'NEWTON': 'https://esearch.newtoncad.org/',
  'NUECES': 'https://esearch.nuecescad.org/',
  'OLDHAM': 'https://esearch.oldhamcad.org/',
  'PARMER': 'https://esearch.parmercad.org/',
  'POLK': 'https://esearch.polkcad.org/',
  'PRESIDIO': 'https://esearch.presidiocad.org/',
  'RAINS': 'https://esearch.rainscad.org/',
  'REAL': 'https://esearch.realcad.org/',
  'ROCKWALL': 'https://esearch.rockwallcad.org/',
  'SCHLEICHER': 'https://esearch.schleichercad.org/',
  'SMITH': 'https://esearch.smithcad.org/',
  'STARR': 'https://esearch.starrcad.org/',
  'TARRANT': 'https://esearch.tarrantcad.org/',
  'TERRELL': 'https://esearch.terrellcad.org/',
  'THROCKMORTON': 'https://esearch.throckmortoncad.org/',
  'UVALDE': 'https://esearch.uvaldecad.org/',
  'VICTORIA': 'https://esearch.victoriacad.org/',
  'WALKER': 'https://esearch.walkercad.org/',
  'WALLER': 'https://esearch.wallercad.org/',
  'WASHINGTON': 'https://esearch.washingtoncad.org/',
  'WILLACY': 'https://esearch.willacycad.org/',
  'WILLIAMSON': 'https://esearch.williamsoncad.org/',
  'YOAKUM': 'https://esearch.yoakumcad.org/',
  'YOUNG': 'https://esearch.youngcad.org/',
};

async function tryBIS(browser, county, existingIds) {
  const code = cadCode(county);
  const bisUrl = BIS_URL_OVERRIDES[county] || `https://esearch.${code}.org/`;
  const page = await browser.newPage();
  page.setDefaultTimeout(60000);
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36');
  await page.setRequestInterception(true);
  page.on('request', req => {
    if (['image','font','media'].includes(req.resourceType())) req.abort();
    else req.continue();
  });

  const records = [];
  const seenIds = new Set(existingIds);

  try {
    // Load BIS homepage
    const resp = await page.goto(bisUrl, { waitUntil: 'networkidle2', timeout: 45000 });
    if (!resp || resp.status() >= 400) { await page.close(); return null; }

    // Wait for JS to render, then check for BIS search form
    await sleep(3000);
    const isBIS = await page.evaluate(() => {
      return !!document.querySelector('select[name=PropertyType]') ||
             !!document.querySelector('input[name=OwnerName]') ||
             !!document.querySelector('input[name="query[owner][name]"]') ||
             document.body.innerText.includes('Property Search');
    });
    if (!isBIS) { log('    BIS: no search form at ' + bisUrl); await page.close(); return null; }

    // Get session token
    const token = await page.evaluate(async () => {
      try {
        const r = await fetch('/search/requestSessionToken', { headers: { 'X-Requested-With': 'XMLHttpRequest' }});
        const d = await r.json();
        return d.searchSessionToken || null;
      } catch(e) { return null; }
    });

    if (!token) { log('    BIS: no session token'); await page.close(); return null; }

    // Search by property type first (gets ALL minerals), then by name terms for extras
    const BIS_SEARCHES = [
      { keywords: 'PropertyType:Mineral', label: 'ALL MINERALS' },
      { keywords: 'OwnerName:ROYALTY', label: 'ROYALTY owners' },
      { keywords: 'OwnerName:PETROLEUM', label: 'PETROLEUM owners' },
    ];
    for (const search of BIS_SEARCHES) {
      try {
        const keywordsRaw = search.keywords;
        const keywords = encodeURIComponent(keywordsRaw);
        const searchUrl = bisUrl + `search/result?keywords=${keywords}&searchSessionToken=${encodeURIComponent(token)}`;

        await page.goto(searchUrl, { waitUntil: 'networkidle0', timeout: 60000 });

        // Wait for grid to load
        await page.waitForFunction(() => {
          const rows = document.querySelectorAll('.k-grid-content tr');
          return rows.length > 0;
        }, { timeout: 20000 }).catch(() => {});

        // Get total results and paginate using the API
        const MAX_PAGES = 500; // No practical limit — get everything
        let pageNum = 1;
        let totalForTerm = 0;

        while (pageNum <= MAX_PAGES) {
          // Use the page's fetchData API to get results for this page
          const pageRecords = await page.evaluate(async (pg, kw) => {
            const searchToken = document.querySelector('meta[name="search-token"]')?.getAttribute('content') || '';
            try {
              const resp = await fetch('./SearchResults?keywords=' + encodeURIComponent(kw), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ page: pg, pageSize: 100, searchToken })
              });
              const result = await resp.json();
              if (!result || result.success === false) return { records: [], total: 0 };
              return {
                total: result.totalResults || result.total || 0,
                records: (result.resultsList || result.results || result.data || []).map(r => ({
                  propertyId: r.propertyId || r.PropertyId || r.prop_id || '',
                  geoId: r.geoId || r.GeoId || r.geo_id || '',
                  type: r.propertyType || r.PropertyType || r.type || '',
                  ownerName: r.ownerName || r.OwnerName || r.owner || '',
                  address: r.situsAddress || r.SitusAddress || r.address || '',
                  legalDescription: r.legalDescription || r.LegalDescription || r.legal || '',
                  appraisedValue: String(r.appraisedValue || r.AppraisedValue || r.appraised || r.marketValue || r.value || 0),
                }))
              };
            } catch(e) { return { records: [], total: 0 }; }
          }, pageNum, keywordsRaw);

          if (!pageRecords.records.length) {
            // Fallback: parse from the rendered grid (page 1 only)
            if (pageNum === 1) {
              const gridRecords = await page.evaluate(() => {
                const rows = document.querySelectorAll('.k-grid-content tr, table tbody tr');
                return Array.from(rows).filter(r => r.querySelectorAll('td').length >= 4).map(r => {
                  const text = r.innerText.split('\t').map(s => s.trim());
                  return {
                    propertyId: text[0] || '', geoId: text[1] || '', type: text[2] || '',
                    ownerName: text[3] || '', address: text[5] || '',
                    legalDescription: text[6] || '', appraisedValue: text[7] || '',
                  };
                });
              });
              pageRecords.records = gridRecords;
            }
            if (!pageRecords.records.length) break;
          }

          let newCount = 0;
          for (const r of pageRecords.records) {
            if (r.propertyId && r.propertyId !== 'Property ID' && !seenIds.has(county + ':' + r.propertyId)) {
              seenIds.add(county + ':' + r.propertyId);
              records.push({
                county, propertyId: r.propertyId, geoId: r.geoId, ownerName: r.ownerName,
                address: r.address, legalDescription: r.legalDescription, marketValue: r.appraisedValue,
                searchTerm: search.label, source: 'bis', scrapedAt: new Date().toISOString(),
              });
              newCount++;
            }
          }
          totalForTerm += newCount;

          if (pageRecords.records.length < 100) break; // Last page
          pageNum++;
          await sleep(1000);
        }

        log(`    BIS "${search.label}": ${totalForTerm} new records (${pageNum} pages)`);
        await sleep(DELAY_MS);
      } catch(e) { log(`    BIS "${term}" error: ${e.message.substring(0, 60)}`); }
    }
  } catch(e) { await page.close().catch(() => {}); return null; }

  await page.close().catch(() => {});
  return records.length > 0 ? records : null;
}

// ===================== MAIN =====================

async function main() {
  logStream = fs.createWriteStream(LOG_FILE, { flags: 'a' });
  log('=================================================================');
  log('Texas CAD Mineral Scraper — ALL PLATFORMS — REAL DATA ONLY');
  log('=================================================================');

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

  log(`Targeting ${counties.length} counties (start: ${startIndex})`);

  const data = loadExisting();
  const existingIds = new Set(data.records.map(r => r.county + ':' + r.propertyId));
  log(`Existing records: ${data.records.length}`);

  let browser = await puppeteer.launch({
    headless: 'shell',
    protocolTimeout: 180000,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
  });

  for (let i = 0; i < counties.length; i++) {
    const county = counties[i];
    const idx = startIndex + i;

    // Skip already successful counties
    if (data.counties[county] && data.counties[county].status === 'success' && data.counties[county].count > 0 && !targetCounty) {
      log(`[${idx+1}/${ALL_COUNTIES.length}] ${county} — already scraped (${data.counties[county].count} records), skipping`);
      continue;
    }

    log(`\n[${idx+1}/${ALL_COUNTIES.length}] ======= ${county} County =======`);

    let records = null;
    let platform = 'none';

    try {
      // Try SDS first
      log('  Trying SDS...');
      records = await trySDS(browser, county, existingIds);
      if (records) platform = 'sds';

      // Try BIS if SDS failed
      if (!records) {
        log('  SDS failed, trying BIS eSearch...');
        records = await tryBIS(browser, county, existingIds);
        if (records) platform = 'bis';
      }

      if (records && records.length > 0) {
        data.records.push(...records);
        records.forEach(r => existingIds.add(county + ':' + r.propertyId));
        data.counties[county] = { count: records.length, status: 'success', platform, scrapedAt: new Date().toISOString() };
        log(`  SUCCESS: ${records.length} records via ${platform}`);
      } else {
        data.counties[county] = { count: 0, status: 'no_data', platform: 'none', scrapedAt: new Date().toISOString() };
        log(`  No mineral data found on either platform`);
      }
    } catch(e) {
      log(`  ERROR: ${e.message.substring(0, 100)}`);
      data.counties[county] = { count: 0, status: 'error', error: e.message.substring(0, 200), scrapedAt: new Date().toISOString() };

      // Relaunch browser on crash
      try { await browser.close(); } catch(_) {}
      browser = await puppeteer.launch({
        headless: 'shell',
        protocolTimeout: 180000,
        args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
      });
    }

    saveProgress(data);
    await sleep(DELAY_MS);
  }

  await browser.close().catch(() => {});

  // Summary
  log('\n=================================================================');
  log('COMPLETE');
  log('=================================================================');
  const succeeded = Object.entries(data.counties).filter(([,v]) => v.count > 0);
  const failed = Object.entries(data.counties).filter(([,v]) => v.count === 0);
  log(`Total records: ${data.records.length}`);
  log(`Counties with data: ${succeeded.length}`);
  log(`Counties without data: ${failed.length}`);
  log(`\nFailed counties:`);
  failed.forEach(([c, v]) => log(`  ${c}: ${v.status} (${v.platform || 'none'})`));
  logStream.end();
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
