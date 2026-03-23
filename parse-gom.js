#!/usr/bin/env node
/**
 * parse-gom.js
 * Parses BSEE Gulf of Mexico offshore data files into JSON.
 *
 * Data source: Bureau of Safety and Environmental Enforcement (BSEE)
 * Files: boreholes, lease owners, pipelines, fields, companies, rigs
 */

const fs = require('fs');
const path = require('path');
const readline = require('readline');

const RAW_DIR = path.join(__dirname, 'data', 'raw', 'gom');
const OUT_DIR = path.join(__dirname, 'data');

// ---------- Utility ----------

function parseCSVLine(line) {
  // Parse a comma-delimited line with quoted fields
  const fields = [];
  let i = 0;
  while (i < line.length) {
    if (line[i] === '"') {
      // Quoted field
      let j = i + 1;
      let value = '';
      while (j < line.length) {
        if (line[j] === '"') {
          if (j + 1 < line.length && line[j + 1] === '"') {
            value += '"';
            j += 2;
          } else {
            j++; // closing quote
            break;
          }
        } else {
          value += line[j];
          j++;
        }
      }
      fields.push(value);
      // Skip comma after closing quote
      if (j < line.length && line[j] === ',') j++;
      i = j;
    } else if (line[i] === ',') {
      fields.push('');
      i++;
    } else {
      // Unquoted field
      let j = i;
      while (j < line.length && line[j] !== ',') j++;
      fields.push(line.substring(i, j));
      if (j < line.length && line[j] === ',') j++;
      i = j;
    }
  }
  return fields;
}

function trim(s) {
  return (s || '').trim();
}

function parseDate(s) {
  // YYYYMMDD or empty
  const d = trim(s);
  if (!d || d.length < 8 || d === '00000000') return '';
  return d.substring(0, 4) + '-' + d.substring(4, 6) + '-' + d.substring(6, 8);
}

function parseMonthDate(s) {
  // "MON-YYYY" format like "OCT-1998"
  const d = trim(s);
  if (!d) return '';
  return d;
}

async function readLines(filePath) {
  const lines = [];
  const rl = readline.createInterface({
    input: fs.createReadStream(filePath, { encoding: 'utf8' }),
    crlfDelay: Infinity,
  });
  for await (const line of rl) {
    if (line.trim()) lines.push(line);
  }
  return lines;
}

// ---------- Parsers ----------

async function parseBoreholes() {
  const file = path.join(RAW_DIR, 'borehole', '5010.txt');
  console.log('Parsing boreholes:', file);

  const lines = await readLines(file);
  const wells = [];

  for (const line of lines) {
    const f = parseCSVLine(line);
    if (f.length < 25) continue;

    const lng = parseFloat(trim(f[20]));
    const lat = parseFloat(trim(f[21]));

    wells.push({
      api: trim(f[0]),
      wellName: trim(f[1]),
      sidetrack: trim(f[2]),
      leaseNum: trim(f[3]),
      areaBlock: trim(f[4]),
      spudDate: parseDate(f[5]),
      operatorCode: trim(f[6]),
      waterDepth: parseInt(trim(f[7]), 10) || 0,
      totalDepthMD: parseInt(trim(f[8]), 10) || 0,
      totalDepthTVD: parseInt(trim(f[9]), 10) || 0,
      surfaceLocation: trim(f[10]),
      surfaceAreaBlock: trim(f[11]),
      bottomLocation: trim(f[12]),
      bottomAreaBlock: trim(f[13]),
      completionDate: parseDate(f[14]),
      statusDate: parseDate(f[15]),
      wellClass: trim(f[16]),   // E=exploration, D=development
      district: trim(f[17]),
      status: trim(f[18]),      // COM, PA, ST, TA, etc.
      fieldCode: trim(f[19]),
      lng: isNaN(lng) ? null : lng,
      lat: isNaN(lat) ? null : lat,
      lngBottom: parseFloat(trim(f[22])) || null,
      latBottom: parseFloat(trim(f[23])) || null,
      operatorCode2: trim(f[24]),
      wellType: f.length > 28 ? trim(f[28]) : '',  // STR, DIR
    });
  }

  console.log(`  Parsed ${wells.length.toLocaleString()} wells`);
  return wells;
}

async function parseLeaseOwners() {
  const file = path.join(RAW_DIR, 'lease-owners', 'lseownddelimit.txt');
  console.log('Parsing lease owners:', file);

  const lines = await readLines(file);
  const records = [];

  for (const line of lines) {
    const f = parseCSVLine(line);
    if (f.length < 9) continue;

    records.push({
      leaseNum: trim(f[0]),
      effectiveDate: parseDate(f[1]),
      leaseDate: parseDate(f[2]),
      ownerType: trim(f[3]),    // T=transfer, C=current
      ownerName: trim(f[4]),
      companyNum: trim(f[5]),
      pctOwnership: parseFloat(trim(f[6])) || 0,
      assignDate: parseDate(f[7]),
      operatorNum: trim(f[8]),
    });
  }

  console.log(`  Parsed ${records.length.toLocaleString()} lease owner records`);
  return records;
}

async function parsePipelines() {
  const file = path.join(RAW_DIR, 'pipelines', 'pplmastdelimit.txt');
  console.log('Parsing pipelines:', file);

  const lines = await readLines(file);
  const records = [];

  for (const line of lines) {
    const f = parseCSVLine(line);
    if (f.length < 30) continue;

    records.push({
      pipelineId: trim(f[0]),
      operatorNum: trim(f[1]),
      segmentName: trim(f[2]),
      startArea: trim(f[3]),
      startBlock: trim(f[4]),
      startLease: trim(f[5]),
      endSegment: trim(f[6]),
      endArea: trim(f[7]),
      endBlock: trim(f[8]),
      endLease: trim(f[9]),
      approvalDate: parseDate(f[11]) || trim(f[11]),
      installDate: parseDate(f[12]) || trim(f[12]),
      statusCode: trim(f[13]),
      productType: trim(f[29]) || '',
      status: trim(f[23]),       // ACT, ABN, etc.
      outerDiameter: parseInt(trim(f[16]), 10) || 0,
      pipelineOperator: trim(f[32]) || trim(f[1]),
    });
  }

  console.log(`  Parsed ${records.length.toLocaleString()} pipelines`);
  return records;
}

async function parseFields() {
  const file = path.join(RAW_DIR, 'fields', 'mastdatadelimit.txt');
  console.log('Parsing fields:', file);

  const lines = await readLines(file);
  const records = [];

  for (const line of lines) {
    const f = parseCSVLine(line);
    if (f.length < 8) continue;

    records.push({
      fieldName: trim(f[0]),
      fieldCode: trim(f[1]),
      area: trim(f[2]),
      block: trim(f[3]),
      blockNum: trim(f[4]),
      operator: trim(f[5]),
      discoveryDate: parseMonthDate(f[6]),
      lastActivityDate: parseMonthDate(f[7]),
      status: trim(f[8]) || '',
    });
  }

  console.log(`  Parsed ${records.length.toLocaleString()} field records`);
  return records;
}

async function parseCompanies() {
  const file = path.join(RAW_DIR, 'companies', 'compalldelimit.txt');
  console.log('Parsing companies:', file);

  const lines = await readLines(file);
  const records = [];

  for (const line of lines) {
    const f = parseCSVLine(line);
    if (f.length < 7) continue;

    records.push({
      companyNum: trim(f[0]),
      effectiveDate: parseDate(f[1]),
      companyName: trim(f[2]),
      companyNameUpper: trim(f[3]),
      terminationDate: parseDate(f[4]),
      companyType: trim(f[5]),        // P=private, G=government, etc.
      businessType: trim(f[6]),
      active: trim(f[7]) || '',
      status: trim(f[8]) || '',
      address1: trim(f[13]) || '',
      address2: trim(f[14]) || '',
      city: trim(f[15]) || '',
      state: trim(f[16]) || '',
      zip: trim(f[17]) || '',
    });
  }

  console.log(`  Parsed ${records.length.toLocaleString()} companies`);
  return records;
}

async function parseRigs() {
  const file = path.join(RAW_DIR, 'rig-list', 'rigidlistdelimit.txt');
  console.log('Parsing rigs:', file);

  const lines = await readLines(file);
  const records = [];

  for (const line of lines) {
    const f = parseCSVLine(line);
    if (f.length < 3) continue;

    records.push({
      rigId: trim(f[0]),
      rigName: trim(f[1]),
      rigType: trim(f[2]),
    });
  }

  console.log(`  Parsed ${records.length.toLocaleString()} rigs`);
  return records;
}

// ---------- Summary Stats ----------

function buildSummary(wells, leases, pipelines, fields, companies, rigs) {
  // Well status breakdown
  const statusCounts = {};
  const operatorCounts = {};
  const areaCounts = {};
  const wellClassCounts = {};
  const wellTypeCounts = {};
  let totalWaterDepth = 0;
  let wellsWithWaterDepth = 0;
  let totalTotalDepth = 0;
  let wellsWithTotalDepth = 0;

  for (const w of wells) {
    const st = w.status || 'UNKNOWN';
    statusCounts[st] = (statusCounts[st] || 0) + 1;

    if (w.operatorCode) {
      operatorCounts[w.operatorCode] = (operatorCounts[w.operatorCode] || 0) + 1;
    }

    // Extract area prefix from areaBlock (e.g., "GI076" -> "GI", "WD027" -> "WD")
    const areaMatch = w.areaBlock.match(/^([A-Z]{2})/);
    if (areaMatch) {
      areaCounts[areaMatch[1]] = (areaCounts[areaMatch[1]] || 0) + 1;
    }

    if (w.wellClass) {
      wellClassCounts[w.wellClass] = (wellClassCounts[w.wellClass] || 0) + 1;
    }

    if (w.wellType) {
      wellTypeCounts[w.wellType] = (wellTypeCounts[w.wellType] || 0) + 1;
    }

    if (w.waterDepth > 0) {
      totalWaterDepth += w.waterDepth;
      wellsWithWaterDepth++;
    }
    if (w.totalDepthMD > 0) {
      totalTotalDepth += w.totalDepthMD;
      wellsWithTotalDepth++;
    }
  }

  // Pipeline stats
  const pipelineStatusCounts = {};
  const pipelineProductCounts = {};
  for (const p of pipelines) {
    const st = p.status || 'UNKNOWN';
    pipelineStatusCounts[st] = (pipelineStatusCounts[st] || 0) + 1;
    if (p.productType) {
      pipelineProductCounts[p.productType] = (pipelineProductCounts[p.productType] || 0) + 1;
    }
  }

  // Field stats
  const fieldStatusCounts = {};
  for (const f of fields) {
    const st = f.status || 'UNKNOWN';
    fieldStatusCounts[st] = (fieldStatusCounts[st] || 0) + 1;
  }

  // Lease stats
  const uniqueLeases = new Set(leases.map(l => l.leaseNum));

  // Top 20 operators by well count (resolved to company names if possible)
  const companyLookup = {};
  for (const c of companies) {
    companyLookup[c.companyNum] = c.companyName;
  }

  const topOperators = Object.entries(operatorCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 50)
    .map(([code, count]) => ({
      code,
      name: companyLookup[code] || code,
      wells: count,
    }));

  // Top areas
  const topAreas = Object.entries(areaCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 30)
    .map(([area, count]) => ({ area, wells: count }));

  return {
    generatedAt: new Date().toISOString(),
    source: 'BSEE (Bureau of Safety and Environmental Enforcement)',
    wells: {
      total: wells.length,
      statuses: statusCounts,
      wellClasses: wellClassCounts,
      wellTypes: wellTypeCounts,
      avgWaterDepthFt: wellsWithWaterDepth ? Math.round(totalWaterDepth / wellsWithWaterDepth) : 0,
      avgTotalDepthFt: wellsWithTotalDepth ? Math.round(totalTotalDepth / wellsWithTotalDepth) : 0,
      withCoordinates: wells.filter(w => w.lat && w.lng).length,
      topOperators,
      topAreas,
    },
    leases: {
      totalRecords: leases.length,
      uniqueLeases: uniqueLeases.size,
    },
    pipelines: {
      total: pipelines.length,
      statuses: pipelineStatusCounts,
      productTypes: pipelineProductCounts,
    },
    fields: {
      total: fields.length,
      statuses: fieldStatusCounts,
    },
    companies: {
      total: companies.length,
    },
    rigs: {
      total: rigs.length,
      types: rigs.reduce((acc, r) => {
        acc[r.rigType] = (acc[r.rigType] || 0) + 1;
        return acc;
      }, {}),
    },
  };
}

// ---------- Main ----------

async function main() {
  console.log('=== BSEE Gulf of Mexico Data Parser ===\n');
  const t0 = Date.now();

  const wells = await parseBoreholes();
  const leases = await parseLeaseOwners();
  const pipelines = await parsePipelines();
  const fields = await parseFields();
  const companies = await parseCompanies();
  const rigs = await parseRigs();

  console.log('\nBuilding summary statistics...');
  const summary = buildSummary(wells, leases, pipelines, fields, companies, rigs);

  // Write output files
  const outputs = [
    ['gom-wells.json', wells],
    ['gom-leases.json', leases],
    ['gom-pipelines.json', pipelines],
    ['gom-fields.json', fields],
    ['gom-companies.json', companies],
    ['gom-rigs.json', rigs],
    ['gom-summary.json', summary],
  ];

  console.log('\nWriting output files...');
  for (const [filename, data] of outputs) {
    const outPath = path.join(OUT_DIR, filename);
    const json = JSON.stringify(data, null, filename === 'gom-summary.json' ? 2 : null);
    fs.writeFileSync(outPath, json, 'utf8');
    const sizeMB = (Buffer.byteLength(json, 'utf8') / (1024 * 1024)).toFixed(2);
    console.log(`  ${filename}: ${sizeMB} MB`);
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`\nDone in ${elapsed}s`);
  console.log(`\nSummary:`);
  console.log(`  Wells:      ${wells.length.toLocaleString()}`);
  console.log(`  Leases:     ${leases.length.toLocaleString()} records (${summary.leases.uniqueLeases.toLocaleString()} unique)`);
  console.log(`  Pipelines:  ${pipelines.length.toLocaleString()}`);
  console.log(`  Fields:     ${fields.length.toLocaleString()}`);
  console.log(`  Companies:  ${companies.length.toLocaleString()}`);
  console.log(`  Rigs:       ${rigs.length.toLocaleString()}`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
