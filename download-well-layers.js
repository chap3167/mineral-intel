const puppeteer = require('puppeteer');
const path = require('path');
const fs = require('fs');

const DOWNLOAD_DIR = '/Users/cchapmn/mineral-data-backup/raw/well_layers';
if (!fs.existsSync(DOWNLOAD_DIR)) fs.mkdirSync(DOWNLOAD_DIR, { recursive: true });

(async () => {
  const browser = await puppeteer.launch({ headless: false, defaultViewport: { width: 1280, height: 900 } });
  const page = await browser.newPage();
  const client = await page.createCDPSession();
  await client.send('Page.setDownloadBehavior', { behavior: 'allow', downloadPath: DOWNLOAD_DIR });

  console.log('Opening RRC Well Layers download page...');
  await page.goto('https://mft.rrc.texas.gov/link/d551fb20-442e-4b67-84fa-ac3f23ecabb4', { waitUntil: 'networkidle2', timeout: 60000 });
  await new Promise(r => setTimeout(r, 5000));

  // Try to find and click download links
  const links = await page.$$('a');
  console.log('Found', links.length, 'links on page');
  for (const a of links) {
    const text = await page.evaluate(e => e.textContent || '', a);
    const href = await page.evaluate(e => e.href || '', a);
    if (text.match(/\.zip|\.shp|well|download/i) || href.match(/\.zip|\.shp/i)) {
      console.log('Clicking:', text.trim().substring(0, 60), '->', href.substring(0, 80));
      await a.click();
      await new Promise(r => setTimeout(r, 5000));
    }
  }

  console.log('Browser will stay open for 3 minutes for download...');
  console.log('Download directory:', DOWNLOAD_DIR);
  await new Promise(r => setTimeout(r, 180000));

  // Check what downloaded
  const files = fs.readdirSync(DOWNLOAD_DIR);
  console.log('Files downloaded:', files.length);
  files.forEach(f => {
    const size = (fs.statSync(path.join(DOWNLOAD_DIR, f)).size / 1024 / 1024).toFixed(1);
    console.log('  ' + f + ' — ' + size + ' MB');
  });

  await browser.close();
})();
