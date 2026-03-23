/**
 * upload-production-fast.js — Fast parallel lease production uploader
 * Sends 5 concurrent batches of 1000 rows for 5x throughput.
 * Skips years already in the database.
 */

const fs = require('fs');
const readline = require('readline');

const SUPABASE_URL = 'https://bkyzxvetrguudtsqrvvq.supabase.co';
const SERVICE_KEY = 'sb_secret__W2xGYHeY8Q8aNZZlOgvEQ_P3yAhSk2';
const BATCH_SIZE = 1000;
const CONCURRENT = 5;
const FILE_PATH = '/Users/cchapmn/mineral-data-backup/raw/pdq_extract/OG_LEASE_CYCLE_DATA_TABLE.dsv';
const MIN_YEAR = 2010;

// Years to skip (already uploaded)
const SKIP_YEARS = new Set([2023, 2024, 2025]);

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
  console.log('=== Fast Lease Production Upload ===');
  console.log(`File: ${FILE_PATH}`);
  console.log(`Min year: ${MIN_YEAR}`);
  console.log(`Skipping years: ${[...SKIP_YEARS].join(', ')}`);
  console.log(`Concurrency: ${CONCURRENT}`);
  console.log('');

  const stream = fs.createReadStream(FILE_PATH, { encoding: 'utf-8' });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let headers = null;
  let batch = [];
  let pendingBatches = [];
  let total = 0;
  let skipped = 0;
  let errors = 0;
  let lineNum = 0;
  const startTime = Date.now();

  for await (const line of rl) {
    lineNum++;
    if (!headers) { headers = line.split('}').map(h => h.trim()); continue; }

    const vals = line.split('}');
    const raw = {};
    headers.forEach((h, i) => { raw[h] = (vals[i] || '').trim(); });

    const year = toInt(raw.CYCLE_YEAR) || 0;
    const oil = toInt(raw.LEASE_OIL_PROD_VOL) || 0;
    const gas = toInt(raw.LEASE_GAS_PROD_VOL) || 0;
    const cond = toInt(raw.LEASE_COND_PROD_VOL) || 0;
    const csgd = toInt(raw.LEASE_CSGD_PROD_VOL) || 0;

    // Skip if no production, before cutoff, or already uploaded year
    if ((oil === 0 && gas === 0 && cond === 0 && csgd === 0) || year < MIN_YEAR || SKIP_YEARS.has(year)) {
      skipped++;
      continue;
    }

    batch.push({
      oil_gas_code: strOrNull(raw.OIL_GAS_CODE),
      district_no: strOrNull(raw.DISTRICT_NO),
      lease_no: strOrNull(raw.LEASE_NO),
      cycle_year: year,
      cycle_month: toInt(raw.CYCLE_MONTH),
      cycle_year_month: strOrNull(raw.CYCLE_YEAR_MONTH),
      operator_no: strOrNull(raw.OPERATOR_NO),
      field_no: strOrNull(raw.FIELD_NO),
      lease_name: strOrNull(raw.LEASE_NAME),
      operator_name: strOrNull(raw.OPERATOR_NAME),
      field_name: strOrNull(raw.FIELD_NAME),
      oil_prod_vol: oil || null,
      gas_prod_vol: gas || null,
      cond_prod_vol: cond || null,
      csgd_prod_vol: csgd || null,
    });

    if (batch.length >= BATCH_SIZE) {
      pendingBatches.push(flush(batch).then(() => { total += BATCH_SIZE; }).catch(e => { errors++; }));
      batch = [];

      // Wait when we hit concurrency limit
      if (pendingBatches.length >= CONCURRENT) {
        await Promise.all(pendingBatches);
        pendingBatches = [];
      }

      if (total % 50000 === 0 && total > 0) {
        const elapsed = (Date.now() - startTime) / 1000;
        const rate = Math.round(total / elapsed);
        const remaining = Math.round((23700000 - total) / rate / 60);
        console.log(`  ${total.toLocaleString()} uploaded, ${skipped.toLocaleString()} skipped, ${errors} errors (${rate}/sec, ~${remaining}min left)`);
      }
    }
  }

  // Flush remaining
  if (batch.length > 0) {
    pendingBatches.push(flush(batch).then(() => { total += batch.length; }).catch(() => { errors++; }));
  }
  await Promise.all(pendingBatches);

  const elapsed = Math.round((Date.now() - startTime) / 1000);
  console.log('');
  console.log('=== DONE ===');
  console.log(`Uploaded: ${total.toLocaleString()}`);
  console.log(`Skipped: ${skipped.toLocaleString()}`);
  console.log(`Errors: ${errors}`);
  console.log(`Time: ${Math.round(elapsed/60)} minutes`);
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
