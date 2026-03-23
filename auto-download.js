const puppeteer = require('puppeteer');
const path = require('path');
const fs = require('fs');

const DOWNLOAD_DIR = path.join(__dirname, 'data', 'raw');

const RRC_LINKS = [
  { name: 'Production Data CSV (1993-present)', url: 'https://mft.rrc.texas.gov/link/1f5ddb8d-329a-4459-b7f8-177b4f5ee60d' },
  { name: 'Drilling Permits with Coordinates', url: 'https://mft.rrc.texas.gov/link/5f07cc72-2e79-4df8-ade1-9aeb792e03fc' },
  { name: 'Statewide API Data (ASCII)', url: 'https://mft.rrc.texas.gov/link/701db9a3-32b5-488d-812b-cd6ff7d0fe85' },
  { name: 'Completion Data (ASCII)', url: 'https://mft.rrc.texas.gov/link/ed7ab066-879f-40b6-8144-2ae4b6810c04' },
  { name: 'P5 Operators (ASCII)', url: 'https://mft.rrc.texas.gov/link/04652169-eed6-4396-9019-2e270e790f6c' },
  { name: 'Wellbore Query Data', url: 'https://mft.rrc.texas.gov/link/650649b7-e019-4d77-a8e0-d118d6455381' },
  { name: 'Drilling Permit Master (ASCII)', url: 'https://mft.rrc.texas.gov/link/e99fbe81-40cd-4a79-b992-9fc71d0f06d4' },
  { name: 'Horizontal Drilling Permits', url: 'https://mft.rrc.texas.gov/link/c725637f-6748-47b9-ad74-e0396879d88b' },
  { name: 'Oil & Gas Field Names', url: 'https://mft.rrc.texas.gov/link/3122a5ec-eb3b-4ed2-908b-f41fa94ab8ba' },
  { name: 'Well Layers by County (GIS)', url: 'https://mft.rrc.texas.gov/link/d551fb20-442e-4b67-84fa-ac3f23ecabb4' },
];

const OTHER_LINKS = [
  { name: 'FracFocus Bulk Data', url: 'https://fracfocus.org/data-download' },
  { name: 'GLO GIS Data', url: 'https://wwwdev.glo.texas.gov/land/gis-maps-and-data' },
  { name: 'Tarrant CAD Mineral Data', url: 'https://www.tad.org/resources/data-downloads' },
];

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function downloadFromRRC(browser, link) {
  console.log(`\n  Downloading: ${link.name}`);
  console.log(`  URL: ${link.url}`);

  const page = await browser.newPage();

  // Set download behavior
  const client = await page.createCDPSession();
  await client.send('Page.setDownloadBehavior', {
    behavior: 'allow',
    downloadPath: DOWNLOAD_DIR,
  });

  try {
    await page.goto(link.url, { waitUntil: 'networkidle2', timeout: 60000 });
    await sleep(3000);

    // Look for download links/buttons on GoDrive portal
    // Try multiple selectors that might be the download button
    const selectors = [
      'a[href*="download"]',
      'button[class*="download"]',
      'a.download',
      '.ui-button',
      'a[title*="Download"]',
      'a[title*="download"]',
      'span.ui-button-text',
      'a[href*=".zip"]',
      'a[href*=".csv"]',
      'a[href*=".dat"]',
      'a[href*=".txt"]',
      // GoDrive specific
      '.file-row a',
      '.file-name a',
      'td a',
      '.ui-datatable-data a',
      'a.file-download',
    ];

    let clicked = false;

    for (const sel of selectors) {
      try {
        const elements = await page.$$(sel);
        if (elements.length > 0) {
          for (const el of elements) {
            const text = await page.evaluate(e => e.textContent || e.title || e.href || '', el);
            const href = await page.evaluate(e => e.href || '', el);
            console.log(`    Found element: "${text.trim().substring(0, 60)}" href=${href ? href.substring(0, 60) : 'none'}`);

            // Click anything that looks like a file download
            if (text.match(/\.(zip|csv|dat|txt|dbf)/i) || href.match(/\.(zip|csv|dat|txt|dbf)/i) || text.match(/download/i)) {
              console.log(`    Clicking: ${text.trim().substring(0, 60)}`);
              await el.click();
              clicked = true;
              await sleep(5000);
              break;
            }
          }
        }
        if (clicked) break;
      } catch (e) {
        // selector not found, try next
      }
    }

    if (!clicked) {
      // Try clicking all links and see what we find
      const allLinks = await page.$$('a');
      console.log(`    Found ${allLinks.length} total links on page`);

      for (const a of allLinks) {
        const href = await page.evaluate(e => e.href || '', a);
        const text = await page.evaluate(e => e.textContent || '', a);
        if (href.match(/\.(zip|csv|dat|txt|dbf|gz)/i)) {
          console.log(`    Clicking file link: ${text.trim()} -> ${href.substring(0, 80)}`);
          await a.click();
          clicked = true;
          await sleep(5000);
          break;
        }
      }
    }

    if (!clicked) {
      // Last resort: take screenshot for debugging
      const screenshotPath = path.join(DOWNLOAD_DIR, `screenshot-${link.name.replace(/[^a-z0-9]/gi, '_')}.png`);
      await page.screenshot({ path: screenshotPath, fullPage: true });
      console.log(`    Could not find download button. Screenshot saved: ${screenshotPath}`);

      // Also dump page content summary
      const title = await page.title();
      const bodyText = await page.evaluate(() => document.body.innerText.substring(0, 500));
      console.log(`    Page title: ${title}`);
      console.log(`    Page text: ${bodyText.substring(0, 200)}`);
    }

    // Wait for download to complete
    await sleep(10000);

  } catch (err) {
    console.log(`    Error: ${err.message}`);
  }

  await page.close();
}

async function downloadFracFocus(browser) {
  console.log('\n  Downloading: FracFocus Bulk Data');
  const page = await browser.newPage();
  const client = await page.createCDPSession();
  await client.send('Page.setDownloadBehavior', {
    behavior: 'allow',
    downloadPath: DOWNLOAD_DIR,
  });

  try {
    await page.goto('https://fracfocus.org/data-download', { waitUntil: 'networkidle2', timeout: 60000 });
    await sleep(3000);

    // Look for CSV download link
    const links = await page.$$('a');
    for (const a of links) {
      const href = await page.evaluate(e => e.href || '', a);
      const text = await page.evaluate(e => e.textContent || '', a);
      if (href.match(/\.(zip|csv)/i) || text.match(/csv|download/i)) {
        console.log(`    Clicking: ${text.trim().substring(0, 60)} -> ${href.substring(0, 80)}`);
        await a.click();
        await sleep(10000);
        break;
      }
    }
  } catch (err) {
    console.log(`    Error: ${err.message}`);
  }

  await page.close();
}

async function main() {
  console.log('=== MineralSearch Auto-Downloader ===');
  console.log(`Download directory: ${DOWNLOAD_DIR}`);

  // Create download dir
  if (!fs.existsSync(DOWNLOAD_DIR)) {
    fs.mkdirSync(DOWNLOAD_DIR, { recursive: true });
  }

  // Launch browser (NOT headless so we can see what's happening and handle any CAPTCHAs)
  const browser = await puppeteer.launch({
    headless: false,
    defaultViewport: { width: 1280, height: 900 },
    args: ['--no-sandbox'],
  });

  console.log('\n--- RRC Downloads ---');
  for (const link of RRC_LINKS) {
    await downloadFromRRC(browser, link);
    await sleep(3000);
  }

  console.log('\n--- FracFocus Download ---');
  await downloadFracFocus(browser);

  // Check what was downloaded
  console.log('\n\n=== Download Summary ===');
  const files = fs.readdirSync(DOWNLOAD_DIR);
  if (files.length === 0) {
    console.log('No files downloaded yet. Check the browser windows.');
  } else {
    files.forEach(f => {
      const stat = fs.statSync(path.join(DOWNLOAD_DIR, f));
      const sizeMB = (stat.size / 1024 / 1024).toFixed(2);
      console.log(`  ${f} — ${sizeMB} MB`);
    });
  }

  console.log('\nBrowser will stay open for 60 seconds in case you need to click anything manually...');
  await sleep(60000);

  await browser.close();
  console.log('Done!');
}

main().catch(console.error);
