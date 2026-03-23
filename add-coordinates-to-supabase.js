/**
 * Update wells in Supabase with real lat/lng coordinates from parsed shapefiles.
 * Matches on API number.
 */

const fs = require('fs');

const SUPABASE_URL = 'https://bkyzxvetrguudtsqrvvq.supabase.co';
const SERVICE_KEY = 'sb_secret__W2xGYHeY8Q8aNZZlOgvEQ_P3yAhSk2';
const COORDS_FILE = '/Users/cchapmn/mineral-intel/data/well-coordinates.json';

async function main() {
  console.log('=== Adding Real Coordinates to Supabase Wells ===');
  console.log('Loading coordinates...');

  const allCoords = JSON.parse(fs.readFileSync(COORDS_FILE, 'utf8'));
  const apiNumbers = Object.keys(allCoords);
  console.log('Wells with coordinates:', apiNumbers.length.toLocaleString());

  // We need to match API numbers between the shapefile format and Supabase format
  // Supabase has: 42-001-00100001
  // Shapefiles have various formats: 4200132761, 00132761, etc.

  // Build a lookup by the last 8 digits (county code + well number)
  const coordsByShortApi = {};
  apiNumbers.forEach(api => {
    const clean = api.replace(/[^0-9]/g, '');
    // Store by last 8 digits
    if (clean.length >= 8) {
      const short = clean.slice(-8);
      coordsByShortApi[short] = allCoords[api];
    }
    // Also store by full API
    coordsByShortApi[clean] = allCoords[api];
  });

  console.log('Lookup entries:', Object.keys(coordsByShortApi).length.toLocaleString());

  // Fetch wells from Supabase in pages and update those that have matching coordinates
  let offset = 0;
  const pageSize = 1000;
  let updated = 0;
  let checked = 0;
  let batchNum = 0;

  while (true) {
    // Fetch a page of wells without coordinates
    const resp = await fetch(
      SUPABASE_URL + '/rest/v1/wells?select=id,api&lat=is.null&limit=' + pageSize + '&offset=' + offset,
      {
        headers: {
          'apikey': SERVICE_KEY,
          'Authorization': 'Bearer ' + SERVICE_KEY,
        }
      }
    );

    if (!resp.ok) {
      console.log('Fetch error:', resp.status);
      break;
    }

    const wells = await resp.json();
    if (wells.length === 0) break;

    checked += wells.length;

    // Build batch update
    const updates = [];
    wells.forEach(well => {
      const apiClean = (well.api || '').replace(/[^0-9]/g, '');
      const short = apiClean.slice(-8);

      const coords = coordsByShortApi[apiClean] || coordsByShortApi[short];
      if (coords && coords.lat && coords.lng) {
        updates.push({ id: well.id, lat: coords.lat, lng: coords.lng });
      }
    });

    // Update in batches of 100
    for (let i = 0; i < updates.length; i += 100) {
      const batch = updates.slice(i, i + 100);
      const promises = batch.map(u =>
        fetch(SUPABASE_URL + '/rest/v1/wells?id=eq.' + u.id, {
          method: 'PATCH',
          headers: {
            'apikey': SERVICE_KEY,
            'Authorization': 'Bearer ' + SERVICE_KEY,
            'Content-Type': 'application/json',
            'Prefer': 'return=minimal',
          },
          body: JSON.stringify({ lat: u.lat, lng: u.lng }),
        }).catch(() => null)
      );
      await Promise.all(promises);
      updated += batch.length;
    }

    batchNum++;
    if (batchNum % 10 === 0) {
      console.log(`  Checked ${checked.toLocaleString()} wells, updated ${updated.toLocaleString()} with coordinates...`);
    }

    offset += pageSize;

    // Safety: if we've checked 1.5M wells, stop
    if (checked > 1500000) break;
  }

  console.log(`\n=== Complete ===`);
  console.log(`Wells checked: ${checked.toLocaleString()}`);
  console.log(`Wells updated with coordinates: ${updated.toLocaleString()}`);
}

main().catch(console.error);
