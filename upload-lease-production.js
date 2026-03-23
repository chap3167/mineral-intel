/**
 * upload-lease-production.js
 *
 * Parses RRC OG_LEASE_CYCLE_DATA_TABLE.dsv and uploads lease-level
 * monthly production data to Supabase. Only uploads rows with
 * actual production (non-zero oil or gas volumes).
 *
 * Usage: node upload-lease-production.js [--recent-only]
 *   --recent-only: Only upload last 10 years of data (saves space)
 */

const fs = require('fs');
const readline = require('readline');

const SUPABASE_URL = 'https://bkyzxvetrguudtsqrvvq.supabase.co';
const SERVICE_KEY = 'sb_secret__W2xGYHeY8Q8aNZZlOgvEQ_P3yAhSk2';
const BATCH_SIZE = 1000;
const FILE_PATH = '/Users/cchapmn/mineral-data-backup/raw/pdq_extract/OG_LEASE_CYCLE_DATA_TABLE.dsv';

const recentOnly = process.argv.includes('--recent-only');
const MIN_YEAR = recentOnly ? 2010 : 0; // Since 2010 if --recent-only

function toInt(val) {
  if (!val || val.trim() === '') return null;
  const n = parseInt(val.trim(), 10);
  return isNaN(n) ? null : n;
}

function strOrNull(val) {
  if (!val || val.trim() === '') return null;
  return val.trim();
}

async function flush(rows) {
  const resp = await fetch(SUPABASE_URL + '/rest/v1/lease_production', {
    method: 'POST',
    headers: {
      'apikey': SERVICE_KEY,
      'Authorization': 'Bearer ' + SERVICE_KEY,
      'Content-Type': 'application/json',
      'Prefer': 'return=minimal',
    },
    body: JSON.stringify(rows),
  });
  if (!resp.ok) {
    const err = await resp.text();
    throw new Error(`Insert failed: ${resp.status} — ${err.substring(0, 200)}`);
  }
}

async function main() {
  console.log('=== Lease Production Upload ===');
  console.log(`File: ${FILE_PATH}`);
  console.log(`Recent only (>=${MIN_YEAR}): ${recentOnly}`);
  console.log('');

  if (!fs.existsSync(FILE_PATH)) {
    console.error('ERROR: File not found');
    process.exit(1);
  }

  const stream = fs.createReadStream(FILE_PATH, { encoding: 'utf-8' });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let headers = null;
  let batch = [];
  let total = 0;
  let skipped = 0;
  let errors = 0;
  let lineNum = 0;

  for await (const line of rl) {
    lineNum++;

    if (!headers) {
      headers = line.split('}').map(h => h.trim());
      continue;
    }

    const vals = line.split('}');
    const raw = {};
    headers.forEach((h, i) => { raw[h] = (vals[i] || '').trim(); });

    // Skip rows with no production
    const oil = toInt(raw.LEASE_OIL_PROD_VOL) || 0;
    const gas = toInt(raw.LEASE_GAS_PROD_VOL) || 0;
    const cond = toInt(raw.LEASE_COND_PROD_VOL) || 0;
    const csgd = toInt(raw.LEASE_CSGD_PROD_VOL) || 0;

    if (oil === 0 && gas === 0 && cond === 0 && csgd === 0) {
      skipped++;
      continue;
    }

    // Skip old data if --recent-only
    const year = toInt(raw.CYCLE_YEAR) || 0;
    if (MIN_YEAR > 0 && year < MIN_YEAR) {
      skipped++;
      continue;
    }

    const row = {
      oil_gas_code:     strOrNull(raw.OIL_GAS_CODE),
      district_no:      strOrNull(raw.DISTRICT_NO),
      lease_no:         strOrNull(raw.LEASE_NO),
      cycle_year:       toInt(raw.CYCLE_YEAR),
      cycle_month:      toInt(raw.CYCLE_MONTH),
      cycle_year_month: strOrNull(raw.CYCLE_YEAR_MONTH),
      operator_no:      strOrNull(raw.OPERATOR_NO),
      field_no:         strOrNull(raw.FIELD_NO),
      lease_name:       strOrNull(raw.LEASE_NAME),
      operator_name:    strOrNull(raw.OPERATOR_NAME),
      field_name:       strOrNull(raw.FIELD_NAME),
      oil_prod_vol:     oil || null,
      gas_prod_vol:     gas || null,
      cond_prod_vol:    cond || null,
      csgd_prod_vol:    csgd || null,
    };

    batch.push(row);

    if (batch.length >= BATCH_SIZE) {
      try {
        await flush(batch);
        total += batch.length;
      } catch (e) {
        errors++;
        if (errors <= 5) console.error(`  Batch error: ${e.message}`);
      }
      batch = [];

      if (total % 100000 === 0) {
        console.log(`  ${total.toLocaleString()} uploaded, ${skipped.toLocaleString()} skipped (line ${lineNum.toLocaleString()})...`);
      }
    }
  }

  // Flush remaining
  if (batch.length > 0) {
    try {
      await flush(batch);
      total += batch.length;
    } catch (e) {
      errors++;
    }
  }

  console.log('');
  console.log('=== DONE ===');
  console.log(`Uploaded: ${total.toLocaleString()}`);
  console.log(`Skipped: ${skipped.toLocaleString()}`);
  console.log(`Errors: ${errors}`);
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
