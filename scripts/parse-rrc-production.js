#!/usr/bin/env node
/**
 * parse-rrc-production.js
 *
 * Parses the RRC Production Data Query (PDQ) dump files.
 * Source: Texas Railroad Commission - PDQ_DSV.zip (3.57 GB compressed, ~35 GB uncompressed)
 * Format: } delimited DSV files (per RRC PDQ Dump User Manual)
 * Data range: January 1993 to present (updated monthly by RRC)
 *
 * RULE #1: ALL DATA IS REAL — from RRC PDQ dump. NO fake data ever.
 *
 * Usage:
 *   node scripts/parse-rrc-production.js              # Full parse
 *   node scripts/parse-rrc-production.js --recent     # Only last 24 months
 *   node scripts/parse-rrc-production.js --summary    # Summary + county stats only
 */

const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { createReadStream, createWriteStream } = require('fs');

const DATA_DIR = path.join(__dirname, '..', 'data', 'rrc-production');
const OUTPUT_DIR = path.join(__dirname, '..', 'data');

const DELIMITER = '}';
const ARGS = process.argv.slice(2);
const RECENT_ONLY = ARGS.includes('--recent');
const SUMMARY_ONLY = ARGS.includes('--summary');

// For --recent mode, only include data from the last 24 months
const now = new Date();
const recentCutoffYear = now.getFullYear() - 2;
const recentCutoffYM = `${recentCutoffYear}${String(now.getMonth() + 1).padStart(2, '0')}`;

// For 12-month stats
const stat12MoCutoffYear = now.getFullYear() - 1;
const stat12MoCutoffYM = `${stat12MoCutoffYear}${String(now.getMonth() + 1).padStart(2, '0')}`;

// ============================================================
// Utility: Stream-parse a DSV file line by line
// ============================================================
async function parseDSV(filePath, onRow, { limit = Infinity } = {}) {
  if (!fs.existsSync(filePath)) {
    console.error(`  File not found: ${filePath}`);
    return { headers: [], rowCount: 0 };
  }

  const fileStream = createReadStream(filePath, { encoding: 'utf-8', highWaterMark: 64 * 1024 });
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

  let headers = null;
  let rowCount = 0;

  for await (const line of rl) {
    if (!headers) {
      headers = line.split(DELIMITER).map(h => h.trim());
      continue;
    }

    if (rowCount >= limit) {
      rl.close();
      fileStream.destroy();
      break;
    }

    const values = line.split(DELIMITER);
    const row = {};
    for (let i = 0; i < headers.length; i++) {
      row[headers[i]] = (values[i] || '').trim();
    }

    onRow(row);
    rowCount++;

    if (rowCount % 5000000 === 0) {
      console.log(`    ... processed ${(rowCount / 1000000).toFixed(0)}M rows`);
      // Force GC if available
      if (global.gc) global.gc();
    }
  }

  return { headers, rowCount };
}

// ============================================================
// Load lookup tables
// ============================================================
async function loadCountyLookup() {
  const counties = {};
  await parseDSV(path.join(DATA_DIR, 'GP_COUNTY_DATA_TABLE.dsv'), (row) => {
    if (row.COUNTY_NO) {
      counties[row.COUNTY_NO] = {
        name: row.COUNTY_NAME || '',
        fips: row.COUNTY_FIPS_CODE || '',
        district: row.DISTRICT_NO || '',
        district_name: row.DISTRICT_NAME || ''
      };
    }
  });
  console.log(`  Loaded ${Object.keys(counties).length} counties`);
  return counties;
}

async function loadOperatorLookup() {
  const operators = {};
  await parseDSV(path.join(DATA_DIR, 'OG_OPERATOR_DW_DATA_TABLE.dsv'), (row) => {
    if (row.OPERATOR_NO) {
      operators[row.OPERATOR_NO] = row.OPERATOR_NAME || '';
    }
  });
  console.log(`  Loaded ${Object.keys(operators).length} operators`);
  return operators;
}

// ============================================================
// Build lease -> county mapping from OG_REGULATORY_LEASE_DW
// ============================================================
async function loadLeaseCountyMap() {
  const leaseCounty = {};
  const filePath = path.join(DATA_DIR, 'OG_REGULATORY_LEASE_DW_DATA_TABLE.dsv');
  if (!fs.existsSync(filePath)) return leaseCounty;

  await parseDSV(filePath, (row) => {
    const key = `${row.OIL_GAS_CODE || ''}-${row.DISTRICT_NO || ''}-${row.LEASE_NO || ''}`;
    // Use county from well data if available
    if (row.COUNTY_NO) {
      leaseCounty[key] = row.COUNTY_NO;
    }
  });
  console.log(`  Loaded county mapping for ${Object.keys(leaseCounty).length} leases`);
  return leaseCounty;
}

// ============================================================
// MAIN PARSE: Stream through OG_LEASE_CYCLE and build outputs
//
// Memory strategy: We stream the 12GB file and write NDJSON output
// incrementally. We accumulate per-lease data in a Map, but only
// keep the metadata + monthly arrays. When done, we flush.
// ============================================================
async function parseAndBuild(counties) {
  const leaseCyclePath = path.join(DATA_DIR, 'OG_LEASE_CYCLE_DATA_TABLE.dsv');
  if (!fs.existsSync(leaseCyclePath)) {
    console.error('ERROR: OG_LEASE_CYCLE_DATA_TABLE.dsv not found!');
    return null;
  }

  const stats = fs.statSync(leaseCyclePath);
  console.log(`  File size: ${(stats.size / 1024 / 1024 / 1024).toFixed(2)} GB`);

  // ---- Aggregation accumulators ----
  let totalRows = 0;
  let oldestYM = '999999';
  let newestYM = '000000';

  // 12-month oil/gas totals
  let oil12Mo = 0, gas12Mo = 0, cond12Mo = 0;

  // County production (last 12 months) - from OG_COUNTY_CYCLE is better,
  // but we can also build it from lease data if needed
  const countyOil12 = {};
  const countyGas12 = {};

  // Per-lease accumulator: Map<leaseKey, { meta, months[] }>
  // This will be large but manageable since we only store numbers
  const leases = new Map();

  // Track recently active leases (production in last 24 months)
  let recentActiveCount = 0;

  console.log('  Streaming OG_LEASE_CYCLE_DATA_TABLE.dsv...');
  const startTime = Date.now();

  await parseDSV(leaseCyclePath, (row) => {
    const cycleYM = row.CYCLE_YEAR_MONTH || '';
    if (!cycleYM || cycleYM.length < 6) return;

    // In --recent mode, skip old data
    if (RECENT_ONLY && cycleYM < recentCutoffYM) return;

    const leaseKey = `${row.OIL_GAS_CODE || ''}-${row.DISTRICT_NO || ''}-${row.LEASE_NO || ''}`;

    // Date range
    if (cycleYM > newestYM) newestYM = cycleYM;
    if (cycleYM < oldestYM) oldestYM = cycleYM;

    const oilVol = parseInt(row.LEASE_OIL_PROD_VOL) || 0;
    const gasVol = parseInt(row.LEASE_GAS_PROD_VOL) || 0;
    const condVol = parseInt(row.LEASE_COND_PROD_VOL) || 0;
    const csgdVol = parseInt(row.LEASE_CSGD_PROD_VOL) || 0;

    // 12-month aggregates
    if (cycleYM >= stat12MoCutoffYM) {
      oil12Mo += oilVol;
      gas12Mo += gasVol;
      cond12Mo += condVol;
    }

    // Build lease record
    if (!SUMMARY_ONLY) {
      let lease = leases.get(leaseKey);
      if (!lease) {
        lease = {
          oil_gas_code: row.OIL_GAS_CODE || '',
          district_no: row.DISTRICT_NO || '',
          lease_no: row.LEASE_NO || '',
          operator_name: row.OPERATOR_NAME || '',
          operator_no: row.OPERATOR_NO || '',
          lease_name: row.LEASE_NAME || '',
          field_name: row.FIELD_NAME || '',
          district_name: row.DISTRICT_NAME || '',
          months: []
        };
        leases.set(leaseKey, lease);
      }

      lease.months.push({
        y: parseInt(row.CYCLE_YEAR) || 0,
        m: parseInt(row.CYCLE_MONTH) || 0,
        o: oilVol,
        g: gasVol,
        c: condVol,
        cg: csgdVol
      });
    }

    totalRows++;
  });

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`  Parsed ${totalRows.toLocaleString()} rows in ${elapsed}s`);
  console.log(`  ${leases.size.toLocaleString()} unique leases`);
  console.log(`  Date range: ${oldestYM} to ${newestYM}`);

  return {
    leases,
    stats: {
      totalRows,
      totalLeases: leases.size,
      oldestYM,
      newestYM,
      oil12Mo,
      gas12Mo,
      cond12Mo,
      parseTimeSec: parseFloat(elapsed)
    }
  };
}

// ============================================================
// Parse county-level production from OG_COUNTY_CYCLE
// ============================================================
async function parseCountyProduction(counties) {
  const filePath = path.join(DATA_DIR, 'OG_COUNTY_CYCLE_DATA_TABLE.dsv');
  if (!fs.existsSync(filePath)) return [];

  const countyTotals = {};

  await parseDSV(filePath, (row) => {
    const cycleYM = row.CYCLE_YEAR_MONTH || '';
    if (cycleYM < stat12MoCutoffYM) return;

    const countyNo = row.COUNTY_NO || '';
    const countyName = row.COUNTY_NAME || (counties[countyNo] || {}).name || `County ${countyNo}`;

    if (!countyTotals[countyNo]) {
      countyTotals[countyNo] = { county_no: countyNo, county_name: countyName, oil: 0, gas: 0, cond: 0 };
    }
    countyTotals[countyNo].oil += parseInt(row.CNTY_OIL_PROD_VOL) || 0;
    countyTotals[countyNo].gas += parseInt(row.CNTY_GAS_PROD_VOL) || 0;
    countyTotals[countyNo].cond += parseInt(row.CNTY_COND_PROD_VOL) || 0;
  });

  return Object.values(countyTotals).sort((a, b) => b.oil - a.oil);
}

// ============================================================
// Write production-by-well outputs
// ============================================================
function writeProductionByWell(leases) {
  if (SUMMARY_ONLY) return { totalLeases: 0, recentLeases: 0 };

  console.log('\nWriting production-by-well files...');

  // Write NDJSON for full streaming access
  const ndjsonPath = path.join(OUTPUT_DIR, 'production-by-well.ndjson');
  const ws = createWriteStream(ndjsonPath);

  let recentCount = 0;
  const recentLeases = []; // For the JSON sample

  for (const [leaseKey, lease] of leases) {
    // Sort months chronologically
    lease.months.sort((a, b) => a.y !== b.y ? a.y - b.y : a.m - b.m);

    const lastMonth = lease.months[lease.months.length - 1];
    const firstMonth = lease.months[0];
    const lastYM = `${lastMonth.y}${String(lastMonth.m).padStart(2, '0')}`;

    const record = {
      lease_key: leaseKey,
      oil_gas_code: lease.oil_gas_code,
      district_no: lease.district_no,
      district_name: lease.district_name,
      lease_no: lease.lease_no,
      lease_name: lease.lease_name,
      operator_no: lease.operator_no,
      operator_name: lease.operator_name,
      field_name: lease.field_name,
      total_months: lease.months.length,
      first_production: `${firstMonth.y}-${String(firstMonth.m).padStart(2, '0')}`,
      last_production: `${lastMonth.y}-${String(lastMonth.m).padStart(2, '0')}`,
      months: lease.months.map(m => ({
        year: m.y,
        month: m.m,
        oil_bbls: m.o,
        gas_mcf: m.g,
        condensate_bbls: m.c,
        casinghead_mcf: m.cg
      }))
    };

    ws.write(JSON.stringify(record) + '\n');

    // Collect recently active leases for the JSON sample
    if (lastYM >= '202401') {
      recentCount++;
      if (recentLeases.length < 50000) {
        recentLeases.push(record);
      }
    }
  }

  ws.end();
  console.log(`  Written ${leases.size.toLocaleString()} leases to ${ndjsonPath}`);

  // Write sampled JSON with recently active leases
  const jsonPath = path.join(OUTPUT_DIR, 'production-by-well.json');
  const sampleOutput = {};
  for (const record of recentLeases) {
    sampleOutput[record.lease_key] = record;
  }
  fs.writeFileSync(jsonPath, JSON.stringify(sampleOutput, null, 2));
  console.log(`  Written ${recentLeases.length.toLocaleString()} recent leases to ${jsonPath}`);

  return { totalLeases: leases.size, recentLeases: recentCount };
}

// ============================================================
// Write production-summary.json
// ============================================================
function writeSummary(prodStats, countyRanking, leaseStats) {
  console.log('\nWriting production-summary.json...');

  function fmtYM(ym) {
    if (!ym || ym.length < 6) return ym;
    const months = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    return `${months[parseInt(ym.substring(4, 6))] || ym.substring(4, 6)} ${ym.substring(0, 4)}`;
  }

  function fmtNum(n) { return (n || 0).toLocaleString('en-US'); }

  const summary = {
    source: 'Texas Railroad Commission - Production Data Query (PDQ) Dump',
    source_url: 'https://mft.rrc.texas.gov/link/1f5ddb8d-329a-4459-b7f8-177b4f5ee60d',
    format_doc: 'https://www.rrc.texas.gov/media/50ypu2cg/pdq-dump-user-manual.pdf',
    generated_at: new Date().toISOString(),
    parse_time_seconds: prodStats.parseTimeSec,
    data_range: {
      oldest_month: prodStats.oldestYM,
      newest_month: prodStats.newestYM,
      oldest_formatted: fmtYM(prodStats.oldestYM),
      newest_formatted: fmtYM(prodStats.newestYM)
    },
    totals: {
      total_production_records: prodStats.totalRows,
      total_leases_with_production: prodStats.totalLeases,
      leases_in_ndjson: leaseStats.totalLeases,
      recent_active_leases: leaseStats.recentLeases
    },
    last_12_months: {
      period_start: stat12MoCutoffYM,
      period_start_formatted: fmtYM(stat12MoCutoffYM),
      total_oil_bbls: prodStats.oil12Mo,
      total_gas_mcf: prodStats.gas12Mo,
      total_condensate_bbls: prodStats.cond12Mo,
      total_oil_formatted: fmtNum(prodStats.oil12Mo) + ' bbls',
      total_gas_formatted: fmtNum(prodStats.gas12Mo) + ' mcf'
    },
    top_producing_counties_12mo: countyRanking.slice(0, 25).map((c, i) => ({
      rank: i + 1,
      county_no: c.county_no,
      county_name: c.county_name,
      oil_bbls: c.oil,
      gas_mcf: c.gas,
      condensate_bbls: c.cond,
      oil_formatted: fmtNum(c.oil) + ' bbls',
      gas_formatted: fmtNum(c.gas) + ' mcf'
    }))
  };

  const outPath = path.join(OUTPUT_DIR, 'production-summary.json');
  fs.writeFileSync(outPath, JSON.stringify(summary, null, 2));
  console.log(`  Written to ${outPath}`);

  // Console summary
  console.log('\n========================================');
  console.log('  RRC PRODUCTION DATA SUMMARY');
  console.log('========================================');
  console.log(`  Data range: ${summary.data_range.oldest_formatted} to ${summary.data_range.newest_formatted}`);
  console.log(`  Total production records: ${fmtNum(summary.totals.total_production_records)}`);
  console.log(`  Total leases: ${fmtNum(summary.totals.total_leases_with_production)}`);
  console.log(`  Last 12 months oil: ${summary.last_12_months.total_oil_formatted}`);
  console.log(`  Last 12 months gas: ${summary.last_12_months.total_gas_formatted}`);
  if (summary.top_producing_counties_12mo.length > 0) {
    console.log('\n  Top 10 Oil Producing Counties (Last 12 Mo):');
    for (const c of summary.top_producing_counties_12mo.slice(0, 10)) {
      console.log(`    ${c.rank}. ${c.county_name.padEnd(20)} ${c.oil_formatted.padStart(20)}  |  ${c.gas_formatted.padStart(20)}`);
    }
  }
  console.log('========================================\n');

  return summary;
}

// ============================================================
// MAIN
// ============================================================
async function main() {
  console.log('=== RRC Production Data Parser ===');
  console.log(`Date: ${new Date().toISOString()}`);
  console.log(`Mode: ${RECENT_ONLY ? 'RECENT (last 24 months)' : SUMMARY_ONLY ? 'SUMMARY ONLY' : 'FULL'}`);
  console.log(`Data dir: ${DATA_DIR}`);
  console.log();

  // Check DSV files exist
  const dsvFiles = fs.readdirSync(DATA_DIR).filter(f => f.endsWith('.dsv'));
  if (dsvFiles.length === 0) {
    console.error('No .dsv files found. Extract PDQ_DSV.zip first.');
    process.exit(1);
  }
  console.log(`Found ${dsvFiles.length} DSV files:`);
  for (const f of dsvFiles) {
    const size = fs.statSync(path.join(DATA_DIR, f)).size;
    console.log(`  ${f} (${(size / 1024 / 1024).toFixed(1)} MB)`);
  }
  console.log();

  // Load lookups
  console.log('[1/4] Loading county lookup...');
  const counties = await loadCountyLookup();

  // Parse county-level production
  console.log('[2/4] Parsing county production (last 12 months)...');
  const countyRanking = await parseCountyProduction(counties);
  console.log(`  ${countyRanking.length} counties with production`);

  // Parse main lease production
  console.log('[3/4] Parsing lease production data...');
  const result = await parseAndBuild(counties);
  if (!result) {
    console.error('Failed to parse production data.');
    process.exit(1);
  }

  // Write outputs
  console.log('[4/4] Writing output files...');
  const leaseStats = writeProductionByWell(result.leases);
  writeSummary(result.stats, countyRanking, leaseStats);

  // Clean up to free memory
  result.leases.clear();

  console.log('Done! Output files:');
  console.log(`  ${path.join(OUTPUT_DIR, 'production-summary.json')}`);
  if (!SUMMARY_ONLY) {
    console.log(`  ${path.join(OUTPUT_DIR, 'production-by-well.json')} (recent leases sample)`);
    console.log(`  ${path.join(OUTPUT_DIR, 'production-by-well.ndjson')} (all leases, streaming)`);
  }
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
