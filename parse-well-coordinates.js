/**
 * Parse ALL county well shapefiles to extract real lat/lng coordinates for every well.
 * Then update the wells in Supabase with real coordinates.
 */

const shapefile = require('shapefile');
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const WELL_LAYERS_DIR = '/Users/cchapmn/mineral-data-backup/raw/well_layers';
const EXTRACT_DIR = '/Users/cchapmn/mineral-data-backup/raw/well_layers_extracted';
const OUTPUT_FILE = '/Users/cchapmn/mineral-intel/data/well-coordinates.json';

const SUPABASE_URL = 'https://bkyzxvetrguudtsqrvvq.supabase.co';
const SERVICE_KEY = 'sb_secret__W2xGYHeY8Q8aNZZlOgvEQ_P3yAhSk2';

async function supabaseUpdate(apiNumber, lat, lng) {
  const resp = await fetch(SUPABASE_URL + '/rest/v1/wells?api=eq.' + encodeURIComponent(apiNumber), {
    method: 'PATCH',
    headers: {
      'apikey': SERVICE_KEY,
      'Authorization': 'Bearer ' + SERVICE_KEY,
      'Content-Type': 'application/json',
      'Prefer': 'return=minimal',
    },
    body: JSON.stringify({ lat: lat, lng: lng }),
  });
  return resp.ok;
}

async function supabaseBatchUpdate(batch) {
  // Supabase doesn't support batch updates easily, so we use individual updates
  // But that's too slow for 1M+ wells. Instead, let's save to a JSON file
  // and create a new approach
  return true;
}

async function parseShapefile(shpPath) {
  const wells = [];
  try {
    const source = await shapefile.open(shpPath);
    let result;
    while (!(result = await source.read()).done) {
      const props = result.value.properties;
      const geom = result.value.geometry;
      if (geom && geom.coordinates) {
        wells.push({
          api: props.API || props.API10 || props.APINUM || '',
          lat: props.LAT83 || geom.coordinates[1],
          lng: props.LONG83 || geom.coordinates[0],
        });
      }
    }
  } catch (e) {
    // Skip files that can't be parsed
  }
  return wells;
}

async function main() {
  console.log('=== Parsing Well Coordinates from County Shapefiles ===');
  console.log('Input directory:', WELL_LAYERS_DIR);

  if (!fs.existsSync(EXTRACT_DIR)) fs.mkdirSync(EXTRACT_DIR, { recursive: true });

  const zipFiles = fs.readdirSync(WELL_LAYERS_DIR).filter(f => f.endsWith('.zip')).sort();
  console.log('Found', zipFiles.length, 'county zip files\n');

  let totalWells = 0;
  let allCoords = {};
  let errors = 0;

  for (let i = 0; i < zipFiles.length; i++) {
    const zipFile = zipFiles[i];
    const countyCode = zipFile.replace('well', '').replace('.zip', '');
    const zipPath = path.join(WELL_LAYERS_DIR, zipFile);
    const extractPath = path.join(EXTRACT_DIR, countyCode);

    // Skip tiny files (likely empty)
    const stat = fs.statSync(zipPath);
    if (stat.size < 100) continue;

    try {
      // Extract the zip
      if (!fs.existsSync(extractPath)) fs.mkdirSync(extractPath, { recursive: true });
      execSync(`unzip -o -q "${zipPath}" -d "${extractPath}" 2>/dev/null`, { stdio: 'pipe' });

      // Find the .shp files (look for bottom-hole or surface files)
      const files = fs.readdirSync(extractPath);
      const shpFiles = files.filter(f => f.endsWith('.shp'));

      for (const shpFile of shpFiles) {
        const shpPath = path.join(extractPath, shpFile);
        const wells = await parseShapefile(shpPath);

        wells.forEach(w => {
          if (w.api && w.lat && w.lng) {
            // Build full API number: 42-{countyCode}-{api}
            let fullApi = w.api;
            if (!fullApi.startsWith('42')) {
              fullApi = '42-' + countyCode.padStart(3, '0') + '-' + w.api.padStart(5, '0');
            }
            // Normalize API format to match Supabase
            if (fullApi.length === 10 && !fullApi.includes('-')) {
              fullApi = '42-' + fullApi.substring(2, 5) + '-' + fullApi.substring(5);
            }
            allCoords[fullApi] = { lat: parseFloat(w.lat), lng: parseFloat(w.lng) };
          }
        });

        totalWells += wells.length;
      }

      if ((i + 1) % 25 === 0) {
        console.log(`  Processed ${i + 1}/${zipFiles.length} counties... ${totalWells.toLocaleString()} wells so far`);
      }

    } catch (e) {
      errors++;
    }

    // Clean up extracted files to save disk space
    try { execSync(`rm -rf "${extractPath}"`, { stdio: 'pipe' }); } catch(e) {}
  }

  console.log(`\n=== Parse Complete ===`);
  console.log(`Counties processed: ${zipFiles.length}`);
  console.log(`Total wells with coordinates: ${Object.keys(allCoords).length.toLocaleString()}`);
  console.log(`Errors: ${errors}`);

  // Save coordinates to JSON
  console.log(`\nSaving to ${OUTPUT_FILE}...`);
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(allCoords));
  const sizeMB = (fs.statSync(OUTPUT_FILE).size / 1024 / 1024).toFixed(1);
  console.log(`Saved: ${sizeMB} MB`);

  // Sample output
  const keys = Object.keys(allCoords);
  console.log(`\nSample coordinates:`);
  for (let i = 0; i < Math.min(5, keys.length); i++) {
    console.log(`  ${keys[i]}: ${allCoords[keys[i]].lat}, ${allCoords[keys[i]].lng}`);
  }

  console.log('\nDone! Now run add-coordinates-to-supabase.js to update the database.');
}

main().catch(console.error);
