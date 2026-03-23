#!/usr/bin/env node
/**
 * scrape-all-cad-minerals.js
 *
 * Comprehensive Texas CAD mineral ownership scraper targeting True Automation
 * property search portals. Scrapes ONLY real data -- never generates fake or
 * fallback records. If a county cannot be scraped, it is skipped and logged.
 *
 * Usage:  node scrape-all-cad-minerals.js [--county midlandcad] [--max-pages 10]
 * Output: data/cad-minerals-real.json
 */

const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DATA_DIR = path.join(__dirname, 'data');
const OUTPUT_FILE = path.join(DATA_DIR, 'cad-minerals-real.json');
const LOG_FILE = path.join(DATA_DIR, 'cad-minerals-scrape.log');

const REQUEST_DELAY_MS = 2500;       // Delay between page loads
const NAV_TIMEOUT_MS = 30000;        // Page navigation timeout
const MAX_PAGES_PER_COUNTY = 50;     // Max result pages to paginate through
const MAX_RETRIES = 2;               // Retries per county on transient failure

// Property categories for mineral interests on True Automation sites
const MINERAL_CATEGORIES = ['G1', 'G2', 'G3'];

// All target counties with their True Automation CAD codes
const COUNTIES = [
  // All 254 Texas counties — True Automation CAD property search
  { name: 'ANDERSON', cadCode: 'andersoncad' },
  { name: 'ANDREWS', cadCode: 'andrewscad' },
  { name: 'ANGELINA', cadCode: 'angelinacad' },
  { name: 'ARANSAS', cadCode: 'aransascad' },
  { name: 'ARCHER', cadCode: 'archercad' },
  { name: 'ARMSTRONG', cadCode: 'armstrongcad' },
  { name: 'ATASCOSA', cadCode: 'atascosacad' },
  { name: 'AUSTIN', cadCode: 'austincad' },
  { name: 'BAILEY', cadCode: 'baileycad' },
  { name: 'BANDERA', cadCode: 'banderacad' },
  { name: 'BASTROP', cadCode: 'bastropcad' },
  { name: 'BAYLOR', cadCode: 'baylorcad' },
  { name: 'BEE', cadCode: 'beecad' },
  { name: 'BELL', cadCode: 'bellcad' },
  { name: 'BEXAR', cadCode: 'bexarcad' },
  { name: 'BLANCO', cadCode: 'blancocad' },
  { name: 'BORDEN', cadCode: 'bordencad' },
  { name: 'BOSQUE', cadCode: 'bosquecad' },
  { name: 'BOWIE', cadCode: 'bowiecad' },
  { name: 'BRAZORIA', cadCode: 'brazoriacad' },
  { name: 'BRAZOS', cadCode: 'brazoscad' },
  { name: 'BREWSTER', cadCode: 'brewstercad' },
  { name: 'BRISCOE', cadCode: 'briscoecad' },
  { name: 'BROOKS', cadCode: 'brookscad' },
  { name: 'BROWN', cadCode: 'browncad' },
  { name: 'BURLESON', cadCode: 'burlesoncad' },
  { name: 'BURNET', cadCode: 'burnetcad' },
  { name: 'CALDWELL', cadCode: 'caldwellcad' },
  { name: 'CALHOUN', cadCode: 'calhouncad' },
  { name: 'CALLAHAN', cadCode: 'callahancad' },
  { name: 'CAMERON', cadCode: 'cameroncad' },
  { name: 'CAMP', cadCode: 'campcad' },
  { name: 'CARSON', cadCode: 'carsoncad' },
  { name: 'CASS', cadCode: 'casscad' },
  { name: 'CASTRO', cadCode: 'castrocad' },
  { name: 'CHAMBERS', cadCode: 'chamberscad' },
  { name: 'CHEROKEE', cadCode: 'cherokeecad' },
  { name: 'CHILDRESS', cadCode: 'childresscad' },
  { name: 'CLAY', cadCode: 'claycad' },
  { name: 'COCHRAN', cadCode: 'cochrancad' },
  { name: 'COKE', cadCode: 'cokecad' },
  { name: 'COLEMAN', cadCode: 'colemancad' },
  { name: 'COLLIN', cadCode: 'collincad' },
  { name: 'COLLINGSWORTH', cadCode: 'collingsworthcad' },
  { name: 'COLORADO', cadCode: 'coloradocad' },
  { name: 'COMAL', cadCode: 'comalcad' },
  { name: 'COMANCHE', cadCode: 'comanchecad' },
  { name: 'CONCHO', cadCode: 'conchocad' },
  { name: 'COOKE', cadCode: 'cookecad' },
  { name: 'CORYELL', cadCode: 'coryellcad' },
  { name: 'COTTLE', cadCode: 'cottlecad' },
  { name: 'CRANE', cadCode: 'cranecad' },
  { name: 'CROCKETT', cadCode: 'crockettcad' },
  { name: 'CROSBY', cadCode: 'crosbycad' },
  { name: 'CULBERSON', cadCode: 'culbersoncad' },
  { name: 'DALLAM', cadCode: 'dallamcad' },
  { name: 'DALLAS', cadCode: 'dallascad' },
  { name: 'DAWSON', cadCode: 'dawsoncad' },
  { name: 'DE WITT', cadCode: 'dewittcad' },
  { name: 'DEAF SMITH', cadCode: 'deafsmithcad' },
  { name: 'DELTA', cadCode: 'deltacad' },
  { name: 'DENTON', cadCode: 'dentoncad' },
  { name: 'DICKENS', cadCode: 'dickenscad' },
  { name: 'DIMMIT', cadCode: 'dimmitcad' },
  { name: 'DONLEY', cadCode: 'donleycad' },
  { name: 'DUVAL', cadCode: 'duvalcad' },
  { name: 'EASTLAND', cadCode: 'eastlandcad' },
  { name: 'ECTOR', cadCode: 'ectorcad' },
  { name: 'EDWARDS', cadCode: 'edwardscad' },
  { name: 'EL PASO', cadCode: 'elpasocad' },
  { name: 'ELLIS', cadCode: 'elliscad' },
  { name: 'ERATH', cadCode: 'erathcad' },
  { name: 'FALLS', cadCode: 'fallscad' },
  { name: 'FANNIN', cadCode: 'fannincad' },
  { name: 'FAYETTE', cadCode: 'fayettecad' },
  { name: 'FISHER', cadCode: 'fishercad' },
  { name: 'FLOYD', cadCode: 'floydcad' },
  { name: 'FOARD', cadCode: 'foardcad' },
  { name: 'FORT BEND', cadCode: 'fortbendcad' },
  { name: 'FRANKLIN', cadCode: 'franklincad' },
  { name: 'FREESTONE', cadCode: 'freestonecad' },
  { name: 'FRIO', cadCode: 'friocad' },
  { name: 'GAINES', cadCode: 'gainescad' },
  { name: 'GALVESTON', cadCode: 'galvestoncad' },
  { name: 'GARZA', cadCode: 'garzacad' },
  { name: 'GILLESPIE', cadCode: 'gillespiecad' },
  { name: 'GLASSCOCK', cadCode: 'glasscockcad' },
  { name: 'GOLIAD', cadCode: 'goliadcad' },
  { name: 'GONZALES', cadCode: 'gonzalescad' },
  { name: 'GRAY', cadCode: 'graycad' },
  { name: 'GRAYSON', cadCode: 'graysoncad' },
  { name: 'GREGG', cadCode: 'greggcad' },
  { name: 'GRIMES', cadCode: 'grimescad' },
  { name: 'GUADALUPE', cadCode: 'guadalupecad' },
  { name: 'HALE', cadCode: 'halecad' },
  { name: 'HALL', cadCode: 'hallcad' },
  { name: 'HAMILTON', cadCode: 'hamiltoncad' },
  { name: 'HANSFORD', cadCode: 'hansfordcad' },
  { name: 'HARDEMAN', cadCode: 'hardemancad' },
  { name: 'HARDIN', cadCode: 'hardincad' },
  { name: 'HARRIS', cadCode: 'harriscad' },
  { name: 'HARRISON', cadCode: 'harrisoncad' },
  { name: 'HARTLEY', cadCode: 'hartleycad' },
  { name: 'HASKELL', cadCode: 'haskellcad' },
  { name: 'HAYS', cadCode: 'hayscad' },
  { name: 'HEMPHILL', cadCode: 'hemphillcad' },
  { name: 'HENDERSON', cadCode: 'hendersoncad' },
  { name: 'HIDALGO', cadCode: 'hidalgocad' },
  { name: 'HILL', cadCode: 'hillcad' },
  { name: 'HOCKLEY', cadCode: 'hockleycad' },
  { name: 'HOOD', cadCode: 'hoodcad' },
  { name: 'HOPKINS', cadCode: 'hopkinscad' },
  { name: 'HOUSTON', cadCode: 'houstoncad' },
  { name: 'HOWARD', cadCode: 'howardcad' },
  { name: 'HUDSPETH', cadCode: 'hudspethcad' },
  { name: 'HUNT', cadCode: 'huntcad' },
  { name: 'HUTCHINSON', cadCode: 'hutchinsoncad' },
  { name: 'IRION', cadCode: 'irioncad' },
  { name: 'JACK', cadCode: 'jackcad' },
  { name: 'JACKSON', cadCode: 'jacksoncad' },
  { name: 'JASPER', cadCode: 'jaspercad' },
  { name: 'JEFF DAVIS', cadCode: 'jeffdaviscad' },
  { name: 'JEFFERSON', cadCode: 'jeffersoncad' },
  { name: 'JIM HOGG', cadCode: 'jimhoggcad' },
  { name: 'JIM WELLS', cadCode: 'jimwellscad' },
  { name: 'JOHNSON', cadCode: 'johnsoncad' },
  { name: 'JONES', cadCode: 'jonescad' },
  { name: 'KARNES', cadCode: 'karnescad' },
  { name: 'KAUFMAN', cadCode: 'kaufmancad' },
  { name: 'KENDALL', cadCode: 'kendallcad' },
  { name: 'KENEDY', cadCode: 'kenedycad' },
  { name: 'KENT', cadCode: 'kentcad' },
  { name: 'KERR', cadCode: 'kerrcad' },
  { name: 'KIMBLE', cadCode: 'kimblecad' },
  { name: 'KING', cadCode: 'kingcad' },
  { name: 'KINNEY', cadCode: 'kinneycad' },
  { name: 'KLEBERG', cadCode: 'klebergcad' },
  { name: 'KNOX', cadCode: 'knoxcad' },
  { name: 'LA SALLE', cadCode: 'lasallecad' },
  { name: 'LAMAR', cadCode: 'lamarcad' },
  { name: 'LAMB', cadCode: 'lambcad' },
  { name: 'LAMPASAS', cadCode: 'lampasascad' },
  { name: 'LAVACA', cadCode: 'lavacacad' },
  { name: 'LEE', cadCode: 'leecad' },
  { name: 'LEON', cadCode: 'leoncad' },
  { name: 'LIBERTY', cadCode: 'libertycad' },
  { name: 'LIMESTONE', cadCode: 'limestonecad' },
  { name: 'LIPSCOMB', cadCode: 'lipscombcad' },
  { name: 'LIVE OAK', cadCode: 'liveoakcad' },
  { name: 'LLANO', cadCode: 'llanocad' },
  { name: 'LOVING', cadCode: 'lovingcad' },
  { name: 'LUBBOCK', cadCode: 'lubbockcad' },
  { name: 'LYNN', cadCode: 'lynncad' },
  { name: 'MADISON', cadCode: 'madisoncad' },
  { name: 'MARION', cadCode: 'marioncad' },
  { name: 'MARTIN', cadCode: 'martincad' },
  { name: 'MASON', cadCode: 'masoncad' },
  { name: 'MATAGORDA', cadCode: 'matagordacad' },
  { name: 'MAVERICK', cadCode: 'maverickcad' },
  { name: 'MCCULLOCH', cadCode: 'mccullochcad' },
  { name: 'MCLENNAN', cadCode: 'mclennancad' },
  { name: 'MCMULLEN', cadCode: 'mcmullencad' },
  { name: 'MEDINA', cadCode: 'medinacad' },
  { name: 'MENARD', cadCode: 'menardcad' },
  { name: 'MIDLAND', cadCode: 'midlandcad' },
  { name: 'MILAM', cadCode: 'milamcad' },
  { name: 'MILLS', cadCode: 'millscad' },
  { name: 'MITCHELL', cadCode: 'mitchellcad' },
  { name: 'MONTAGUE', cadCode: 'montaguecad' },
  { name: 'MONTGOMERY', cadCode: 'montgomerycad' },
  { name: 'MOORE', cadCode: 'moorecad' },
  { name: 'MORRIS', cadCode: 'morriscad' },
  { name: 'MOTLEY', cadCode: 'motleycad' },
  { name: 'NACOGDOCHES', cadCode: 'nacogdochescad' },
  { name: 'NAVARRO', cadCode: 'navarrocad' },
  { name: 'NEWTON', cadCode: 'newtoncad' },
  { name: 'NOLAN', cadCode: 'nolancad' },
  { name: 'NUECES', cadCode: 'nuecescad' },
  { name: 'OCHILTREE', cadCode: 'ochiltreecad' },
  { name: 'OLDHAM', cadCode: 'oldhamcad' },
  { name: 'ORANGE', cadCode: 'orangecad' },
  { name: 'PALO PINTO', cadCode: 'palopintocad' },
  { name: 'PANOLA', cadCode: 'panolacad' },
  { name: 'PARKER', cadCode: 'parkercad' },
  { name: 'PARMER', cadCode: 'parmercad' },
  { name: 'PECOS', cadCode: 'pecoscad' },
  { name: 'POLK', cadCode: 'polkcad' },
  { name: 'POTTER', cadCode: 'pottercad' },
  { name: 'PRESIDIO', cadCode: 'presidiocad' },
  { name: 'RAINS', cadCode: 'rainscad' },
  { name: 'RANDALL', cadCode: 'randallcad' },
  { name: 'REAGAN', cadCode: 'reagancad' },
  { name: 'REAL', cadCode: 'realcad' },
  { name: 'RED RIVER', cadCode: 'redrivercad' },
  { name: 'REEVES', cadCode: 'reevescad' },
  { name: 'REFUGIO', cadCode: 'refugiocad' },
  { name: 'ROBERTS', cadCode: 'robertscad' },
  { name: 'ROBERTSON', cadCode: 'robertsoncad' },
  { name: 'ROCKWALL', cadCode: 'rockwallcad' },
  { name: 'RUNNELS', cadCode: 'runnelscad' },
  { name: 'RUSK', cadCode: 'ruskcad' },
  { name: 'SABINE', cadCode: 'sabinecad' },
  { name: 'SAN AUGUSTINE', cadCode: 'sanaugustinecad' },
  { name: 'SAN JACINTO', cadCode: 'sanjacintocad' },
  { name: 'SAN PATRICIO', cadCode: 'sanpatriciocad' },
  { name: 'SAN SABA', cadCode: 'sansabacad' },
  { name: 'SCHLEICHER', cadCode: 'schleichercad' },
  { name: 'SCURRY', cadCode: 'scurrycad' },
  { name: 'SHACKELFORD', cadCode: 'shackelfordcad' },
  { name: 'SHELBY', cadCode: 'shelbycad' },
  { name: 'SHERMAN', cadCode: 'shermancad' },
  { name: 'SMITH', cadCode: 'smithcad' },
  { name: 'SOMERVELL', cadCode: 'somervellcad' },
  { name: 'STARR', cadCode: 'starrcad' },
  { name: 'STEPHENS', cadCode: 'stephenscad' },
  { name: 'STERLING', cadCode: 'sterlingcad' },
  { name: 'STONEWALL', cadCode: 'stonewallcad' },
  { name: 'SUTTON', cadCode: 'suttoncad' },
  { name: 'SWISHER', cadCode: 'swishercad' },
  { name: 'TARRANT', cadCode: 'tarrantcad' },
  { name: 'TAYLOR', cadCode: 'taylorcad' },
  { name: 'TERRELL', cadCode: 'terrellcad' },
  { name: 'TERRY', cadCode: 'terrycad' },
  { name: 'THROCKMORTON', cadCode: 'throckmortoncad' },
  { name: 'TITUS', cadCode: 'tituscad' },
  { name: 'TOM GREEN', cadCode: 'tomgreencad' },
  { name: 'TRAVIS', cadCode: 'traviscad' },
  { name: 'TRINITY', cadCode: 'trinitycad' },
  { name: 'TYLER', cadCode: 'tylercad' },
  { name: 'UPSHUR', cadCode: 'upshurcad' },
  { name: 'UPTON', cadCode: 'uptoncad' },
  { name: 'UVALDE', cadCode: 'uvaldecad' },
  { name: 'VAL VERDE', cadCode: 'valverdecad' },
  { name: 'VAN ZANDT', cadCode: 'vanzandtcad' },
  { name: 'VICTORIA', cadCode: 'victoriacad' },
  { name: 'WALKER', cadCode: 'walkercad' },
  { name: 'WALLER', cadCode: 'wallercad' },
  { name: 'WARD', cadCode: 'wardcad' },
  { name: 'WASHINGTON', cadCode: 'washingtoncad' },
  { name: 'WEBB', cadCode: 'webbcad' },
  { name: 'WHARTON', cadCode: 'whartoncad' },
  { name: 'WHEELER', cadCode: 'wheelercad' },
  { name: 'WICHITA', cadCode: 'wichitacad' },
  { name: 'WILBARGER', cadCode: 'wilbargercad' },
  { name: 'WILLACY', cadCode: 'willacycad' },
  { name: 'WILLIAMSON', cadCode: 'williamsoncad' },
  { name: 'WILSON', cadCode: 'wilsoncad' },
  { name: 'WINKLER', cadCode: 'winklercad' },
  { name: 'WISE', cadCode: 'wisecad' },
  { name: 'WOOD', cadCode: 'woodcad' },
  { name: 'YOAKUM', cadCode: 'yoakumcad' },
  { name: 'YOUNG', cadCode: 'youngcad' },
  { name: 'ZAPATA', cadCode: 'zapatacad' },
  { name: 'ZAVALA', cadCode: 'zavalacad' },
];

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

let logStream;

function log(msg) {
  const ts = new Date().toISOString();
  const line = '[' + ts + '] ' + msg;
  console.log(line);
  if (logStream) logStream.write(line + '\n');
}

function logError(msg) {
  const ts = new Date().toISOString();
  const line = '[' + ts + '] ERROR: ' + msg;
  console.error(line);
  if (logStream) logStream.write(line + '\n');
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function parseArgs() {
  const args = process.argv.slice(2);
  const opts = { county: null, maxPages: MAX_PAGES_PER_COUNTY };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--county' && args[i + 1]) {
      opts.county = args[++i].toLowerCase();
    }
    if (args[i] === '--max-pages' && args[i + 1]) {
      opts.maxPages = parseInt(args[++i], 10) || MAX_PAGES_PER_COUNTY;
    }
  }
  return opts;
}

/**
 * Load existing output file so we can resume after crashes.
 */
function loadExistingData() {
  try {
    if (fs.existsSync(OUTPUT_FILE)) {
      const raw = fs.readFileSync(OUTPUT_FILE, 'utf8');
      return JSON.parse(raw);
    }
  } catch (e) {
    log('Could not load existing data: ' + e.message);
  }
  return null;
}

/**
 * Save current state to disk (called after each county).
 */
function saveProgress(data) {
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(data, null, 2));
}

// ---------------------------------------------------------------------------
// True Automation search URL builders
// ---------------------------------------------------------------------------

function buildSearchUrls(cadCode) {
  const upper = cadCode.toUpperCase();
  return [
    // Southwest Data Solutions (most common in TX now)
    'https://www.southwestdatasolution.com/webindex.aspx?dbkey=' + upper,
    'https://iswdataclient.azurewebsites.net/webindex.aspx?dbkey=' + upper,
    // True Automation (legacy, some counties still use)
    'https://propaccess.trueautomation.com/' + cadCode + '/search.php',
    'https://propaccess.trueautomation.com/' + cadCode + '/',
    // Esearch pattern
    'https://esearch.' + cadCode.replace('cad', '') + 'cad.org/',
  ];
}

// ---------------------------------------------------------------------------
// Core scraping logic
// ---------------------------------------------------------------------------

/**
 * Attempt to load the True Automation search page for a county.
 * Returns the working URL or null if none responds.
 */
async function findSearchPage(page, cadCode) {
  const urls = buildSearchUrls(cadCode);

  for (const url of urls) {
    try {
      log('  Trying URL: ' + url);
      const resp = await page.goto(url, {
        waitUntil: 'domcontentloaded',
        timeout: NAV_TIMEOUT_MS,
      });

      if (!resp || resp.status() >= 400) {
        log('  Got HTTP ' + (resp ? resp.status() : 'null') + ' from ' + url);
        continue;
      }

      // Verify this looks like a property search page (any platform)
      const isSearchPage = await page.evaluate(() => {
        const html = document.documentElement.innerHTML || '';
        return (
          html.includes('trueautomation') ||
          html.includes('TrueAutomation') ||
          html.includes('search.php') ||
          html.includes('Property Search') ||
          html.includes('prop_id') ||
          html.includes('owner_name') ||
          html.includes('southwestdata') ||
          html.includes('SouthwestData') ||
          html.includes('iswdata') ||
          html.includes('webSearchAddress') ||
          html.includes('webSearchOwner') ||
          html.includes('Appraisal District') ||
          html.includes('Property ID') ||
          html.includes('Owner Name')
        );
      });

      if (isSearchPage) {
        log('  Found property search page: ' + url);
        return url;
      }

      // Check for CAPTCHA or block
      const isBlocked = await page.evaluate(() => {
        const text = (document.body?.innerText || '').toLowerCase();
        return (
          text.includes('captcha') ||
          text.includes('access denied') ||
          text.includes('blocked') ||
          text.includes('rate limit') ||
          text.includes('cloudflare')
        );
      });

      if (isBlocked) {
        log('  Blocked/CAPTCHA detected at ' + url + ' -- skipping county');
        return null;
      }
    } catch (err) {
      log('  Failed to load ' + url + ': ' + err.message);
    }
  }

  return null;
}

/**
 * On the True Automation search page, submit a search filtered by mineral
 * property category. Returns true if the search was submitted successfully.
 */
async function submitMineralSearch(page, searchUrl, category) {
  try {
    // Navigate to the search page fresh for each category
    await page.goto(searchUrl, {
      waitUntil: 'domcontentloaded',
      timeout: NAV_TIMEOUT_MS,
    });
    await sleep(1000);

    // Strategy 1: Look for a property type/category dropdown and select the mineral category
    const hasPropertyTypeDropdown = await page.evaluate((cat) => {
      const selects = document.querySelectorAll('select');
      for (const sel of selects) {
        const name = (sel.name || sel.id || '').toLowerCase();
        if (
          name.includes('type') ||
          name.includes('category') ||
          name.includes('stype') ||
          name.includes('prop_type')
        ) {
          const options = Array.from(sel.options);
          for (const opt of options) {
            const val = (opt.value || '').toUpperCase();
            const txt = (opt.text || '').toUpperCase();
            if (val === cat || txt.includes(cat) || txt.includes('MINERAL') || txt.includes('OIL') || txt.includes('GAS')) {
              sel.value = opt.value;
              sel.dispatchEvent(new Event('change', { bubbles: true }));
              return { found: true, selectedValue: opt.value, selectedText: opt.text };
            }
          }
        }
      }
      return { found: false };
    }, category);

    if (hasPropertyTypeDropdown.found) {
      log('    Selected property type: ' + hasPropertyTypeDropdown.selectedText + ' (' + hasPropertyTypeDropdown.selectedValue + ')');
    }

    // Strategy 2: Check for search_type radio buttons or tabs
    await page.evaluate((cat) => {
      const radios = document.querySelectorAll('input[type="radio"]');
      for (const radio of radios) {
        const val = (radio.value || '').toLowerCase();
        if (val.includes('type') || val.includes('category') || val.includes('prop_type')) {
          radio.checked = true;
          radio.dispatchEvent(new Event('change', { bubbles: true }));
          return true;
        }
      }
      return false;
    }, category);

    // Strategy 3: Look for a property type text input
    const hasPropertyTypeInput = await page.evaluate((cat) => {
      const inputs = document.querySelectorAll('input[type="text"]');
      for (const inp of inputs) {
        const name = (inp.name || inp.id || '').toLowerCase();
        if (name.includes('prop_type') || name.includes('property_type') || name.includes('stype')) {
          inp.value = cat;
          inp.dispatchEvent(new Event('input', { bubbles: true }));
          return true;
        }
      }
      return false;
    }, category);

    if (hasPropertyTypeInput) {
      log('    Set property type input to: ' + category);
    }

    // If no filter found, try advanced search
    if (!hasPropertyTypeDropdown.found && !hasPropertyTypeInput) {
      const clickedAdvanced = await page.evaluate(() => {
        const links = document.querySelectorAll('a, button');
        for (const link of links) {
          const text = (link.innerText || link.textContent || '').toLowerCase();
          if (text.includes('advanced') || text.includes('detailed') || text.includes('by type')) {
            link.click();
            return true;
          }
        }
        return false;
      });

      if (clickedAdvanced) {
        await sleep(1500);
        log('    Clicked advanced search link');

        await page.evaluate((cat) => {
          const selects = document.querySelectorAll('select');
          for (const sel of selects) {
            const options = Array.from(sel.options);
            for (const opt of options) {
              const val = (opt.value || '').toUpperCase();
              const txt = (opt.text || '').toUpperCase();
              if (val === cat || txt.includes(cat) || txt.includes('MINERAL') || txt.includes('OIL')) {
                sel.value = opt.value;
                sel.dispatchEvent(new Event('change', { bubbles: true }));
                return { found: true, selectedText: opt.text };
              }
            }
          }
          return { found: false };
        }, category);
      }
    }

    // Submit the search form
    const submitted = await page.evaluate(() => {
      const selectors = [
        'input[type="submit"]',
        'button[type="submit"]',
        'input[value="Search"]',
        'input[value="search"]',
        'button',
      ];
      for (const sel of selectors) {
        const btns = document.querySelectorAll(sel);
        for (const btn of btns) {
          const text = (btn.value || btn.innerText || '').toLowerCase();
          if (text.includes('search') || text.includes('submit') || text.includes('find')) {
            btn.click();
            return true;
          }
        }
      }
      const forms = document.querySelectorAll('form');
      if (forms.length > 0) {
        forms[0].submit();
        return true;
      }
      return false;
    });

    if (!submitted) {
      log('    Could not find submit button for search');
      return false;
    }

    // Wait for results to load
    try {
      await page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS });
    } catch (e) {
      await sleep(3000);
    }

    await sleep(1500);

    // Check page status
    const pageStatus = await page.evaluate(() => {
      const text = (document.body?.innerText || '').toLowerCase();
      if (text.includes('captcha') || text.includes('access denied') || text.includes('blocked')) {
        return 'blocked';
      }
      if (text.includes('no results') || text.includes('no records') || text.includes('0 results')) {
        return 'no_results';
      }
      if (text.includes('too many') || text.includes('narrow your search') || text.includes('refine')) {
        return 'too_broad';
      }
      return 'ok';
    });

    if (pageStatus === 'blocked') {
      log('    Search blocked by CAPTCHA/access control');
      return false;
    }

    if (pageStatus === 'no_results') {
      log('    No results for category ' + category);
      return false;
    }

    if (pageStatus === 'too_broad') {
      log('    Search too broad for category ' + category + ' -- will try pagination anyway');
    }

    return true;
  } catch (err) {
    logError('submitMineralSearch failed: ' + err.message);
    return false;
  }
}

/**
 * Extract mineral property records from the current results page.
 */
async function extractRecordsFromPage(page, countyName) {
  const records = await page.evaluate((county) => {
    const results = [];

    // True Automation results are typically in a table
    const tables = document.querySelectorAll('table');
    let resultTable = null;

    // Find the results table (usually has property data headers)
    for (const table of tables) {
      const rows = table.querySelectorAll('tr');
      if (rows.length > 2) {
        const headerText = (rows[0]?.innerText || '').toLowerCase();
        if (
          headerText.includes('account') ||
          headerText.includes('owner') ||
          headerText.includes('property') ||
          headerText.includes('value') ||
          headerText.includes('legal')
        ) {
          resultTable = table;
          break;
        }
      }
    }

    // Fallback: try the largest table
    if (!resultTable) {
      let maxRows = 0;
      for (const table of tables) {
        const rows = table.querySelectorAll('tr');
        if (rows.length > maxRows) {
          maxRows = rows.length;
          resultTable = table;
        }
      }
    }

    if (!resultTable) {
      // Try div-based results
      const resultDivs = document.querySelectorAll('.search-result, .result-row, .property-row, [class*="result"]');
      for (const div of resultDivs) {
        const text = div.innerText || '';
        const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
        if (lines.length >= 2) {
          results.push({
            propertyId: lines[0] || '',
            ownerName: lines[1] || '',
            legalDescription: lines.slice(2).join(' '),
            assessedValue: '',
            propertyType: '',
          });
        }
      }
      return results;
    }

    // Parse the result table
    const rows = resultTable.querySelectorAll('tr');
    if (rows.length < 2) return results;

    // Determine column indices from the header row
    const headerCells = rows[0].querySelectorAll('th, td');
    const colMap = {};
    headerCells.forEach((cell, idx) => {
      const text = (cell.innerText || '').toLowerCase().trim();
      if (text.includes('account') || (text.includes('prop') && text.includes('id'))) colMap.propertyId = idx;
      if (text.includes('owner')) colMap.ownerName = idx;
      if (text.includes('legal') || text.includes('description') || text.includes('property desc')) colMap.legalDescription = idx;
      if (text.includes('value') || text.includes('market') || text.includes('assessed') || text.includes('appraised')) {
        if (colMap.assessedValue === undefined) colMap.assessedValue = idx;
      }
      if (text.includes('type') || text.includes('category') || text.includes('class')) colMap.propertyType = idx;
      if (text.includes('address') || text.includes('situs')) colMap.address = idx;
    });

    // Parse data rows
    for (let i = 1; i < rows.length; i++) {
      const cells = rows[i].querySelectorAll('td');
      if (cells.length < 2) continue;

      const firstLink = cells[0]?.querySelector('a');
      const propertyId = (
        (colMap.propertyId !== undefined ? cells[colMap.propertyId]?.innerText : '') ||
        (firstLink ? firstLink.innerText : '') ||
        cells[0]?.innerText ||
        ''
      ).trim();

      const ownerName = (
        (colMap.ownerName !== undefined ? cells[colMap.ownerName]?.innerText : '') ||
        cells[1]?.innerText ||
        ''
      ).trim();

      const legalDescription = (
        (colMap.legalDescription !== undefined ? cells[colMap.legalDescription]?.innerText : '') ||
        (colMap.address !== undefined ? cells[colMap.address]?.innerText : '') ||
        (cells.length > 2 ? cells[2]?.innerText : '') ||
        ''
      ).trim();

      const valueText = (
        (colMap.assessedValue !== undefined ? cells[colMap.assessedValue]?.innerText : '') ||
        ''
      ).trim();

      const propertyType = (
        (colMap.propertyType !== undefined ? cells[colMap.propertyType]?.innerText : '') ||
        ''
      ).trim();

      // Skip header-like rows or empty rows
      if (!propertyId || propertyId.toLowerCase().includes('account') || propertyId.length < 2) continue;
      if (!ownerName || ownerName.length < 2) continue;

      results.push({
        propertyId,
        ownerName,
        legalDescription,
        assessedValue: valueText,
        propertyType,
      });
    }

    return results;
  }, countyName);

  return records;
}

/**
 * Check if there's a "Next" page link and navigate to it.
 */
async function goToNextPage(page) {
  try {
    const hasNext = await page.evaluate(() => {
      const links = document.querySelectorAll('a');
      for (const link of links) {
        const text = (link.innerText || link.textContent || '').trim().toLowerCase();
        if (
          text === 'next' ||
          text === '>' ||
          text === '>>' ||
          text === 'next page' ||
          (text.includes('next') && !text.includes('previous'))
        ) {
          const parent = link.parentElement;
          const isDisabled = (
            link.classList.contains('disabled') ||
            (parent && parent.classList.contains('disabled')) ||
            (parent && parent.classList.contains('active'))
          );
          if (!isDisabled) {
            link.click();
            return true;
          }
        }
      }
      return false;
    });

    if (hasNext) {
      try {
        await page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS });
      } catch (e) {
        await sleep(3000);
      }
      await sleep(REQUEST_DELAY_MS);
      return true;
    }

    return false;
  } catch (err) {
    return false;
  }
}

/**
 * Alternative approach: search by owner name patterns common in mineral records.
 */
async function searchByMineralOwnerTerms(page, searchUrl, countyName) {
  const allRecords = [];
  const seenIds = new Set();

  const searchTerms = ['MINERAL', 'ROYALTY', 'MIN INT', 'MINERALS'];

  for (const term of searchTerms) {
    try {
      log('    Searching owner name: "' + term + '"');
      await page.goto(searchUrl, {
        waitUntil: 'domcontentloaded',
        timeout: NAV_TIMEOUT_MS,
      });
      await sleep(1000);

      // Find the owner name input and type the search term
      const ownerInputFilled = await page.evaluate((searchTerm) => {
        const inputs = document.querySelectorAll('input[type="text"]');
        for (const inp of inputs) {
          const name = (inp.name || inp.id || '').toLowerCase();
          if (name.includes('owner') || name.includes('name')) {
            inp.value = searchTerm;
            inp.dispatchEvent(new Event('input', { bubbles: true }));
            inp.dispatchEvent(new Event('change', { bubbles: true }));
            return true;
          }
        }
        // Fallback: try the second text input (first is often prop_id)
        if (inputs.length >= 2) {
          inputs[1].value = searchTerm;
          inputs[1].dispatchEvent(new Event('input', { bubbles: true }));
          return true;
        }
        return false;
      }, term);

      if (!ownerInputFilled) {
        log('    Could not find owner name input field');
        continue;
      }

      // Submit
      const submitted = await page.evaluate(() => {
        const btn = document.querySelector(
          'input[type="submit"], button[type="submit"], input[value="Search"]'
        );
        if (btn) { btn.click(); return true; }
        const form = document.querySelector('form');
        if (form) { form.submit(); return true; }
        return false;
      });

      if (!submitted) continue;

      try {
        await page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS });
      } catch (e) {
        await sleep(3000);
      }
      await sleep(1500);

      // Check for blocking or no results
      const status = await page.evaluate(() => {
        const text = (document.body?.innerText || '').toLowerCase();
        if (text.includes('captcha') || text.includes('blocked') || text.includes('access denied')) return 'blocked';
        if (text.includes('no results') || text.includes('no records') || text.includes('0 results')) return 'no_results';
        return 'ok';
      });

      if (status === 'blocked') {
        log('    Blocked -- stopping owner searches');
        break;
      }
      if (status === 'no_results') {
        log('    No results for "' + term + '"');
        continue;
      }

      // Extract records from this page and paginate
      let pageNum = 1;
      while (pageNum <= MAX_PAGES_PER_COUNTY) {
        const records = await extractRecordsFromPage(page, countyName);
        let newCount = 0;
        for (const rec of records) {
          if (!seenIds.has(rec.propertyId)) {
            seenIds.add(rec.propertyId);
            allRecords.push(rec);
            newCount++;
          }
        }
        log('      Page ' + pageNum + ': ' + records.length + ' rows, ' + newCount + ' new');

        if (records.length === 0) break;

        const hasNext = await goToNextPage(page);
        if (!hasNext) break;
        pageNum++;
      }

      await sleep(REQUEST_DELAY_MS);
    } catch (err) {
      logError('Owner search for "' + term + '" failed: ' + err.message);
    }
  }

  return allRecords;
}

/**
 * Main scraping routine for one county.
 * Returns { status, records[], error? }
 */
async function scrapeCounty(browser, county, opts) {
  const { name, cadCode } = county;
  log('\n' + '='.repeat(60));
  log('Scraping ' + name + ' County (' + cadCode + ')');
  log('='.repeat(60));

  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(NAV_TIMEOUT_MS);
  page.setDefaultTimeout(NAV_TIMEOUT_MS);
  await page.setUserAgent(
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 ' +
    '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
  );
  // Block images and stylesheets for speed
  await page.setRequestInterception(true);
  page.on('request', (req) => {
    const type = req.resourceType();
    if (type === 'image' || type === 'stylesheet' || type === 'font' || type === 'media') {
      req.abort();
    } else {
      req.continue();
    }
  });

  const allRecords = [];
  const seenIds = new Set();
  let status = 'failed';
  let errorMsg = '';

  try {
    // Step 1: Find the search page
    const searchUrl = await findSearchPage(page, cadCode);
    if (!searchUrl) {
      log('  Could not find search page for ' + name + ' -- SKIPPING');
      await page.close();
      return { status: 'no_search_page', records: [], error: 'Search page not found or not accessible' };
    }

    // Step 2: Try searching by mineral property categories (G1, G2, G3)
    for (const category of MINERAL_CATEGORIES) {
      log('  Searching category: ' + category);
      const searchOk = await submitMineralSearch(page, searchUrl, category);
      if (!searchOk) {
        log('  Category ' + category + ' search failed or returned no results');
        continue;
      }

      // Extract records and paginate
      let pageNum = 1;
      while (pageNum <= opts.maxPages) {
        const records = await extractRecordsFromPage(page, name);
        let newCount = 0;
        for (const rec of records) {
          if (!seenIds.has(rec.propertyId)) {
            seenIds.add(rec.propertyId);
            rec.propertyType = rec.propertyType || category;
            allRecords.push(rec);
            newCount++;
          }
        }
        log('    Page ' + pageNum + ': ' + records.length + ' rows, ' + newCount + ' new (total: ' + allRecords.length + ')');

        if (records.length === 0) break;

        const hasNext = await goToNextPage(page);
        if (!hasNext) break;
        pageNum++;
      }

      await sleep(REQUEST_DELAY_MS);
    }

    // Step 3: If category search didn't work, try owner name approach
    if (allRecords.length === 0) {
      log('  Category search yielded no results, trying owner name search...');
      const ownerRecords = await searchByMineralOwnerTerms(page, searchUrl, name);
      for (const rec of ownerRecords) {
        if (!seenIds.has(rec.propertyId)) {
          seenIds.add(rec.propertyId);
          allRecords.push(rec);
        }
      }
    }

    if (allRecords.length > 0) {
      status = 'success';
      log('  SUCCESS: ' + allRecords.length + ' mineral records scraped from ' + name);
    } else {
      status = 'no_records';
      log('  No mineral records found for ' + name + ' (search page was accessible but no data extracted)');
    }
  } catch (err) {
    status = 'error';
    errorMsg = err.message;
    logError('Scraping ' + name + ' failed: ' + err.message);
  } finally {
    try { await page.close(); } catch (_) {}
  }

  // Format records for output
  const formattedRecords = allRecords.map(rec => {
    let assessedValue = 0;
    if (rec.assessedValue) {
      const cleaned = String(rec.assessedValue).replace(/[^0-9.]/g, '');
      assessedValue = parseFloat(cleaned) || 0;
      if (assessedValue > 0 && assessedValue < 1) assessedValue = 0;
      assessedValue = Math.round(assessedValue);
    }

    return {
      county: name,
      propertyId: rec.propertyId,
      ownerName: rec.ownerName,
      legalDescription: rec.legalDescription,
      assessedValue,
      propertyType: rec.propertyType || '',
      source: 'trueautomation',
      scrapedAt: new Date().toISOString(),
    };
  });

  return { status, records: formattedRecords, error: errorMsg || undefined };
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

(async () => {
  const opts = parseArgs();

  log('=================================================================');
  log('Texas CAD Mineral Ownership Scraper (REAL DATA ONLY)');
  log('Started: ' + new Date().toISOString());
  log('=================================================================');
  log('');
  log('IMPORTANT: This scraper saves ONLY real data from actual CAD websites.');
  log('If a county cannot be scraped, it is skipped. NO FAKE DATA EVER.');
  log('');

  // Ensure output directory exists
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

  // Open log file
  logStream = fs.createWriteStream(LOG_FILE, { flags: 'a' });

  // Determine which counties to scrape
  let targetCounties = COUNTIES;
  if (opts.county) {
    targetCounties = COUNTIES.filter(c =>
      c.cadCode === opts.county || c.name.toLowerCase() === opts.county.replace('cad', '')
    );
    if (targetCounties.length === 0) {
      logError('County "' + opts.county + '" not found in target list.');
      logError('Available: ' + COUNTIES.map(c => c.cadCode).join(', '));
      process.exit(1);
    }
  }

  log('Targeting ' + targetCounties.length + ' counties');
  log('Max pages per county: ' + opts.maxPages);
  log('');

  // Load existing data for resume capability
  let outputData = loadExistingData();
  if (outputData && outputData.records && outputData.records.length > 0) {
    log('Loaded existing data: ' + outputData.records.length + ' records from previous run');
  } else {
    outputData = {
      scrapedAt: new Date().toISOString(),
      totalRecords: 0,
      counties: {},
      records: [],
    };
  }

  // Launch browser
  let browser;
  try {
    browser = await puppeteer.launch({
      headless: 'new',
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--disable-extensions',
        '--window-size=1280,800',
      ],
    });
  } catch (err) {
    logError('Failed to launch Puppeteer: ' + err.message);
    process.exit(1);
  }

  const results = {};

  for (const county of targetCounties) {
    let attempt = 0;
    let result = null;

    while (attempt <= MAX_RETRIES) {
      if (attempt > 0) {
        log('  Retry ' + attempt + '/' + MAX_RETRIES + ' for ' + county.name + '...');
        await sleep(5000);
      }

      result = await scrapeCounty(browser, county, opts);

      // If we got records or the failure is not transient, stop retrying
      if (result.status === 'success' || result.status === 'no_records' || result.status === 'no_search_page') {
        break;
      }

      attempt++;
    }

    // Store results
    results[county.name] = {
      count: result.records.length,
      status: result.status,
      error: result.error || undefined,
    };

    // Update output data
    outputData.counties[county.name] = results[county.name];

    if (result.records.length > 0) {
      // Remove any existing records for this county (in case of re-run)
      outputData.records = outputData.records.filter(r => r.county !== county.name);
      outputData.records.push(...result.records);
    }

    outputData.totalRecords = outputData.records.length;
    outputData.scrapedAt = new Date().toISOString();

    // Save progress after each county
    saveProgress(outputData);
    log('  Progress saved: ' + outputData.totalRecords + ' total records');

    // Delay between counties
    await sleep(REQUEST_DELAY_MS);
  }

  await browser.close();

  // Final save
  saveProgress(outputData);

  // Print summary
  log('');
  log('=================================================================');
  log('SCRAPE COMPLETE');
  log('=================================================================');
  log('Output: ' + OUTPUT_FILE);
  log('Log:    ' + LOG_FILE);
  log('Total real records: ' + outputData.totalRecords);
  log('');
  log('County Summary:');
  log('-'.repeat(55));

  let successCount = 0;
  let failCount = 0;

  for (const county of targetCounties) {
    const r = results[county.name] || { count: 0, status: 'unknown' };
    const statusIcon = r.status === 'success' ? 'OK' : 'SKIP';
    log('  ' + county.name.padEnd(14) + String(r.count).padStart(6) + ' records  [' + statusIcon + '] ' + r.status + (r.error ? ' - ' + r.error : ''));
    if (r.status === 'success') successCount++;
    else failCount++;
  }

  log('-'.repeat(55));
  log('  Counties scraped: ' + successCount + '/' + targetCounties.length);
  log('  Counties skipped: ' + failCount);
  log('  Total records:    ' + outputData.totalRecords);
  log('');

  if (outputData.totalRecords === 0) {
    log('WARNING: No records were scraped from any county.');
    log('This may indicate the True Automation sites have changed their layout,');
    log('are blocking automated access, or are currently down.');
    log('Check the log file for details on each county attempt.');
  }

  logStream.end();
})();
