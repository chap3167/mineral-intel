/**
 * upload-all-data.js
 *
 * Parses RRC DSV files (} delimited) and uploads them to Supabase
 * in batches of 1000. Uses streaming to handle large files.
 *
 * Usage:  node upload-all-data.js
 */

const fs = require('fs');
const readline = require('readline');
const { createClient } = require('@supabase/supabase-js');

// --------------- Config ---------------
const SUPABASE_URL = 'https://bkyzxvetrguudtsqrvvq.supabase.co';
const SUPABASE_KEY = 'sb_secret__W2xGYHeY8Q8aNZZlOgvEQ_P3yAhSk2';
const BATCH_SIZE = 1000;
const LOG_EVERY = 10000;
const BASE_DIR = '/Users/cchapmn/mineral-data-backup/raw/pdq_extract';

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// --------------- Helpers ---------------

/**
 * Stream-parse a DSV file line by line, transform each row, and
 * upsert into Supabase in batches.
 */
async function processFile({ filePath, tableName, transform, upsertKey }) {
  console.log(`\n========== ${tableName} ==========`);
  console.log(`File: ${filePath}`);

  if (!fs.existsSync(filePath)) {
    console.error(`  ERROR: File not found — ${filePath}`);
    return 0;
  }

  const stream = fs.createReadStream(filePath, { encoding: 'utf-8' });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let headers = null;
  let batch = [];
  let total = 0;
  let errors = 0;

  for await (const line of rl) {
    // First line is the header row
    if (!headers) {
      headers = line.split('}').map(h => h.trim());
      continue;
    }

    const values = line.split('}');
    // Build a plain object keyed by header name
    const raw = {};
    headers.forEach((h, i) => {
      raw[h] = (values[i] || '').trim();
    });

    // Transform into the shape matching the Supabase table
    const row = transform(raw);
    if (!row) continue; // skip if transform returns null

    batch.push(row);

    if (batch.length >= BATCH_SIZE) {
      const err = await flush(tableName, batch, upsertKey);
      errors += err;
      total += batch.length;
      batch = [];
      if (total % LOG_EVERY === 0) {
        console.log(`  … ${total.toLocaleString()} rows uploaded`);
      }
    }
  }

  // Flush remaining
  if (batch.length > 0) {
    const err = await flush(tableName, batch, upsertKey);
    errors += err;
    total += batch.length;
  }

  console.log(`  DONE — ${total.toLocaleString()} rows uploaded (${errors} batch errors)`);
  return total;
}

/**
 * Insert (or upsert) a batch of rows into Supabase.
 * Returns 0 on success, 1 on error.
 */
async function flush(tableName, rows, upsertKey) {
  try {
    let query;
    if (upsertKey) {
      query = supabase.from(tableName).upsert(rows, { onConflict: upsertKey, ignoreDuplicates: true });
    } else {
      query = supabase.from(tableName).insert(rows);
    }
    const { error } = await query;
    if (error) {
      console.error(`  BATCH ERROR (${tableName}): ${error.message}`);
      return 1;
    }
    return 0;
  } catch (err) {
    console.error(`  BATCH EXCEPTION (${tableName}): ${err.message}`);
    return 1;
  }
}

/** Convert a string to an integer, returning null if empty / NaN. */
function toInt(val) {
  if (!val || val === '') return null;
  const n = parseInt(val, 10);
  return isNaN(n) ? null : n;
}

/** Return trimmed string or null if empty. */
function strOrNull(val) {
  if (!val || val.trim() === '') return null;
  return val.trim();
}

// --------------- Table Definitions ---------------

const TABLES = [
  {
    filePath: `${BASE_DIR}/OG_COUNTY_CYCLE_DATA_TABLE.dsv`,
    tableName: 'production_county',
    upsertKey: null,
    transform: (r) => ({
      county_no:       strOrNull(r.COUNTY_NO),
      county_name:     strOrNull(r.COUNTY_NAME),
      district_no:     strOrNull(r.DISTRICT_NO),
      cycle_year:      toInt(r.CYCLE_YEAR),
      cycle_month:     toInt(r.CYCLE_MONTH),
      cycle_year_month: strOrNull(r.CYCLE_YEAR_MONTH),
      oil_prod_vol:    toInt(r.CNTY_OIL_PROD_VOL),
      gas_prod_vol:    toInt(r.CNTY_GAS_PROD_VOL),
      cond_prod_vol:   toInt(r.CNTY_COND_PROD_VOL),
      csgd_prod_vol:   toInt(r.CNTY_CSGD_PROD_VOL),
    }),
  },
  {
    filePath: `${BASE_DIR}/OG_OPERATOR_DW_DATA_TABLE.dsv`,
    tableName: 'operators',
    upsertKey: 'operator_no',
    transform: (r) => ({
      operator_no:    strOrNull(r.OPERATOR_NO),
      operator_name:  strOrNull(r.OPERATOR_NAME),
      status_code:    strOrNull(r.P5_STATUS_CODE),
      last_filed_date: strOrNull(r.P5_LAST_FILED_DT),
    }),
  },
  {
    filePath: `${BASE_DIR}/OG_SUMMARY_ONSHORE_LEASE_DATA_TABLE.dsv`,
    tableName: 'leases',
    upsertKey: null,
    transform: (r) => ({
      oil_gas_code:  strOrNull(r.OIL_GAS_CODE),
      district_no:   strOrNull(r.DISTRICT_NO),
      lease_no:      strOrNull(r.LEASE_NO),
      operator_no:   strOrNull(r.OPERATOR_NO),
      operator_name: strOrNull(r.OPERATOR_NAME),
      field_no:      strOrNull(r.FIELD_NO),
      field_name:    strOrNull(r.FIELD_NAME),
      lease_name:    strOrNull(r.LEASE_NAME),
      cycle_min:     strOrNull(r.CYCLE_YEAR_MONTH_MIN),
      cycle_max:     strOrNull(r.CYCLE_YEAR_MONTH_MAX),
    }),
  },
  {
    filePath: `${BASE_DIR}/OG_REGULATORY_LEASE_DW_DATA_TABLE.dsv`,
    tableName: 'lease_details',
    upsertKey: null,
    transform: (r) => ({
      oil_gas_code:  strOrNull(r.OIL_GAS_CODE),
      district_no:   strOrNull(r.DISTRICT_NO),
      district_name: strOrNull(r.DISTRICT_NAME),
      lease_no:      strOrNull(r.LEASE_NO),
      lease_name:    strOrNull(r.LEASE_NAME),
      operator_no:   strOrNull(r.OPERATOR_NO),
      operator_name: strOrNull(r.OPERATOR_NAME),
      field_no:      strOrNull(r.FIELD_NO),
      field_name:    strOrNull(r.FIELD_NAME),
      well_no:       strOrNull(r.WELL_NO),
    }),
  },
  {
    filePath: `${BASE_DIR}/OG_FIELD_DW_DATA_TABLE.dsv`,
    tableName: 'fields',
    upsertKey: 'field_no',
    transform: (r) => ({
      field_no:      strOrNull(r.FIELD_NO),
      field_name:    strOrNull(r.FIELD_NAME),
      district_no:   strOrNull(r.DISTRICT_NO),
      district_name: strOrNull(r.DISTRICT_NAME),
      field_class:   strOrNull(r.FIELD_CLASS),
    }),
  },
];

// --------------- Main ---------------

async function main() {
  console.log('MineralSearch — RRC Data Upload');
  console.log(`Supabase: ${SUPABASE_URL}`);
  console.log(`Batch size: ${BATCH_SIZE}`);
  console.log(`Source dir: ${BASE_DIR}\n`);

  const summary = {};

  for (const table of TABLES) {
    const count = await processFile(table);
    summary[table.tableName] = count;
  }

  console.log('\n========== SUMMARY ==========');
  for (const [name, count] of Object.entries(summary)) {
    console.log(`  ${name}: ${count.toLocaleString()} rows`);
  }
  console.log('Done.');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
