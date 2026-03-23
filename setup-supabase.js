/**
 * Setup Supabase database: create tables and upload 1.3M wells
 * Uses the REST API directly (no SDK needed for table creation)
 */

const fs = require('fs');
const readline = require('readline');
const path = require('path');

const SUPABASE_URL = 'https://bkyzxvetrguudtsqrvvq.supabase.co';
const SERVICE_KEY = 'sb_secret__W2xGYHeY8Q8aNZZlOgvEQ_P3yAhSk2';
const ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJreXp4dmV0cmd1dWR0c3FydnZxIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM4ODcyNTUsImV4cCI6MjA4OTQ2MzI1NX0.FE2YEIk9pu6P5EUdQY7S7YRfQK0Q7ckSGuJDnWxSks8';

const WELLBORE_FILE = path.join(__dirname, '..', 'mineral-data-backup', 'parsed-wellbores.json');

async function supabaseSQL(sql) {
  const resp = await fetch(`${SUPABASE_URL}/rest/v1/rpc/`, {
    method: 'POST',
    headers: {
      'apikey': SERVICE_KEY,
      'Authorization': `Bearer ${SERVICE_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query: sql }),
  });
  return resp;
}

async function supabaseInsert(table, rows) {
  const resp = await fetch(`${SUPABASE_URL}/rest/v1/${table}`, {
    method: 'POST',
    headers: {
      'apikey': SERVICE_KEY,
      'Authorization': `Bearer ${SERVICE_KEY}`,
      'Content-Type': 'application/json',
      'Prefer': 'return=minimal',
    },
    body: JSON.stringify(rows),
  });
  if (!resp.ok) {
    const err = await resp.text();
    throw new Error(`Insert failed: ${resp.status} — ${err.substring(0, 200)}`);
  }
  return resp;
}

async function createTable() {
  console.log('Creating wells table via SQL Editor...');
  console.log('');
  console.log('NOTE: You need to run this SQL in Supabase SQL Editor:');
  console.log('Go to: https://supabase.com/dashboard → Your project → SQL Editor');
  console.log('Paste and run:');
  console.log('');
  console.log(`
-- Create wells table
CREATE TABLE IF NOT EXISTS wells (
  id SERIAL PRIMARY KEY,
  api TEXT NOT NULL,
  district TEXT,
  county TEXT,
  well_type TEXT,
  lease_name TEXT,
  field_name TEXT,
  well_number TEXT,
  operator TEXT,
  operator_num TEXT,
  profile TEXT,
  total_depth INTEGER,
  status TEXT,
  offshore BOOLEAN DEFAULT FALSE
);

-- Create indexes for fast searching
CREATE INDEX IF NOT EXISTS idx_wells_county ON wells(county);
CREATE INDEX IF NOT EXISTS idx_wells_operator ON wells(operator);
CREATE INDEX IF NOT EXISTS idx_wells_status ON wells(status);
CREATE INDEX IF NOT EXISTS idx_wells_api ON wells(api);
CREATE INDEX IF NOT EXISTS idx_wells_well_type ON wells(well_type);

-- Full text search index
CREATE INDEX IF NOT EXISTS idx_wells_search ON wells USING gin(
  to_tsvector('english', coalesce(county,'') || ' ' || coalesce(operator,'') || ' ' || coalesce(lease_name,'') || ' ' || coalesce(api,''))
);

-- Enable read access for anonymous users (public search)
ALTER TABLE wells ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow public read" ON wells FOR SELECT USING (true);

-- Stats view
CREATE OR REPLACE VIEW well_stats AS
SELECT
  count(*) as total_wells,
  count(*) FILTER (WHERE status = 'PRODUCING') as producing_wells,
  count(DISTINCT county) as counties,
  count(DISTINCT operator) as operators,
  count(*) FILTER (WHERE offshore = true) as offshore_wells
FROM wells;
  `);
  console.log('');
  console.log('After running the SQL, come back and run this script again with --upload flag.');
}

async function uploadWells() {
  console.log('=== Uploading wells to Supabase ===');
  console.log(`Reading: ${WELLBORE_FILE}`);

  if (!fs.existsSync(WELLBORE_FILE)) {
    console.log('ERROR: parsed-wellbores.json not found at expected path.');
    console.log('Expected:', WELLBORE_FILE);
    return;
  }

  const fileSize = (fs.statSync(WELLBORE_FILE).size / 1024 / 1024).toFixed(1);
  console.log(`File size: ${fileSize} MB`);
  console.log('Streaming and uploading in batches of 1000...');

  const rl = readline.createInterface({
    input: fs.createReadStream(WELLBORE_FILE),
    crlfDelay: Infinity,
  });

  let batch = [];
  let total = 0;
  let errors = 0;
  let batchNum = 0;
  let inArray = false;

  for await (const line of rl) {
    const trimmed = line.trim();

    // Skip array brackets
    if (trimmed === '[' || trimmed === ']') {
      inArray = trimmed === '[';
      continue;
    }

    // Parse each JSON object
    let cleanLine = trimmed;
    if (cleanLine.endsWith(',')) cleanLine = cleanLine.slice(0, -1);

    try {
      const well = JSON.parse(cleanLine);

      batch.push({
        api: well.api || '',
        district: well.dist || '',
        county: well.county || '',
        well_type: well.type || '',
        lease_name: well.lease || '',
        field_name: well.field || '',
        well_number: well.well || '',
        operator: well.op || '',
        operator_num: well.opNum || '',
        profile: well.profile || '',
        total_depth: well.depth || 0,
        status: well.status || '',
        offshore: false,
      });

      if (batch.length >= 1000) {
        batchNum++;
        try {
          await supabaseInsert('wells', batch);
          total += batch.length;
          if (batchNum % 50 === 0) {
            console.log(`  Uploaded ${total.toLocaleString()} wells (batch ${batchNum})...`);
          }
        } catch (e) {
          errors++;
          if (errors <= 3) console.log(`  Batch ${batchNum} error: ${e.message}`);
          if (errors === 4) console.log('  (suppressing further error messages)');
        }
        batch = [];
      }
    } catch (e) {
      // Skip unparseable lines
    }
  }

  // Upload remaining
  if (batch.length > 0) {
    try {
      await supabaseInsert('wells', batch);
      total += batch.length;
    } catch (e) {
      errors++;
    }
  }

  console.log(`\n=== Upload Complete ===`);
  console.log(`Total uploaded: ${total.toLocaleString()}`);
  console.log(`Errors: ${errors}`);
  console.log(`Batches: ${batchNum}`);

  // Now upload GOM wells
  const gomFile = path.join(__dirname, 'data', 'gom-wells.json');
  if (fs.existsSync(gomFile)) {
    console.log('\nUploading GOM offshore wells...');
    const gomData = JSON.parse(fs.readFileSync(gomFile, 'utf8'));
    const gomWells = (Array.isArray(gomData) ? gomData : gomData.wells || []);

    let gomBatch = [];
    let gomTotal = 0;

    for (const w of gomWells) {
      gomBatch.push({
        api: w.api_well_number || w.api || '',
        district: '',
        county: w.surface_area || w.area || 'GOM',
        well_type: w.type_code || '',
        lease_name: w.well_name || '',
        field_name: w.field || '',
        well_number: w.sidetrack_code || '',
        operator: w.operator || w.bus_asc_name || '',
        operator_num: '',
        profile: w.type || '',
        total_depth: parseInt(w.total_depth_md) || parseInt(w.total_depth) || 0,
        status: w.status_code || w.status || '',
        offshore: true,
      });

      if (gomBatch.length >= 1000) {
        try {
          await supabaseInsert('wells', gomBatch);
          gomTotal += gomBatch.length;
        } catch (e) {}
        gomBatch = [];
      }
    }

    if (gomBatch.length > 0) {
      try {
        await supabaseInsert('wells', gomBatch);
        gomTotal += gomBatch.length;
      } catch (e) {}
    }

    console.log(`GOM wells uploaded: ${gomTotal.toLocaleString()}`);
  }
}

// Main
const args = process.argv.slice(2);
if (args.includes('--upload')) {
  uploadWells().catch(console.error);
} else {
  createTable();
}
