#!/usr/bin/env node
/**
 * build-site-data.js
 * Reads the large parsed-wellbores.json (268MB) via streaming,
 * filters to active wells, and produces a compact site-data.json
 * suitable for browser loading (target: under 10MB).
 *
 * Strategy:
 * - Include only PRODUCING wells (~282K) in the individual wells array
 * - Use compact tuple format with minimal fields
 * - Include full aggregate stats for ALL 1.3M wells
 * - Shut-in / injection wells appear in aggregate stats only
 */

const fs = require('fs');
const path = require('path');

const DATA_DIR = path.join(__dirname, 'data');
const WELLBORE_FILE = path.join(DATA_DIR, 'parsed-wellbores.json');
const OUTPUT_FILE = path.join(DATA_DIR, 'site-data.json');

// Only PRODUCING wells go into the browsable array
const INCLUDE_IN_ARRAY = new Set(['PRODUCING']);

// All "active" statuses for stats
const ACTIVE_STATUSES = new Set([
  'PRODUCING', 'SHUT IN', 'SHUT IN-MULTI-COMPL', 'INJECTION', 'PROD FACTOR WELL',
]);

async function streamWellbores() {
  console.log('Streaming wellbores from', WELLBORE_FILE, '...');

  // Compact wells: [api, county, type, lease, op, depth, status]
  // We'll deduplicate long operator names via a lookup table
  const wells = [];
  const countyMap = {};     // all wells
  const operatorMap = {};   // all wells
  const statusMap = {};     // all wells
  const typeMap = {};
  const fieldMap = {};

  // Track active-only stats
  const activeCountyMap = {};
  const activeOperatorMap = {};

  let totalRead = 0;
  let totalKept = 0;

  // String interning for operators to save space in JSON
  const opIndex = {};  // operator name -> index
  const opList = [];   // index -> operator name
  let opNextId = 0;

  function getOpId(name) {
    if (name in opIndex) return opIndex[name];
    const id = opNextId++;
    opIndex[name] = id;
    opList.push(name);
    return id;
  }

  // String interning for counties
  const cntyIndex = {};
  const cntyList = [];
  let cntyNextId = 0;

  function getCntyId(name) {
    if (name in cntyIndex) return cntyIndex[name];
    const id = cntyNextId++;
    cntyIndex[name] = id;
    cntyList.push(name);
    return id;
  }

  const content = fs.createReadStream(WELLBORE_FILE, { encoding: 'utf8', highWaterMark: 128 * 1024 });
  let buffer = '';

  function processWell(w) {
    totalRead++;

    if (totalRead % 200000 === 0) {
      process.stdout.write(`  Read ${(totalRead / 1000).toFixed(0)}K wells, kept ${(totalKept / 1000).toFixed(0)}K...\r`);
    }

    const st = (w.status || '').toUpperCase();
    statusMap[st || '(blank)'] = (statusMap[st || '(blank)'] || 0) + 1;

    const county = w.county || '';
    if (county) countyMap[county] = (countyMap[county] || 0) + 1;

    const op = w.op || '';
    if (op) operatorMap[op] = (operatorMap[op] || 0) + 1;

    const wellType = w.type || '';
    if (wellType) typeMap[wellType] = (typeMap[wellType] || 0) + 1;

    const field = w.field || '';
    if (field) fieldMap[field] = (fieldMap[field] || 0) + 1;

    // Track active well stats
    if (ACTIVE_STATUSES.has(st)) {
      if (county) activeCountyMap[county] = (activeCountyMap[county] || 0) + 1;
      if (op) activeOperatorMap[op] = (activeOperatorMap[op] || 0) + 1;
    }

    // Only include producing wells in the browsable array
    if (!INCLUDE_IN_ARRAY.has(st)) return;

    totalKept++;

    // Compact tuple using interned IDs for county and operator
    // [api, countyId, type, opId, depth]
    // Strip "42-" prefix from API (all TX wells) to save bytes
    const api = (w.api || '').replace(/^42-/, '');
    wells.push([
      api,
      getCntyId(county),
      wellType,
      getOpId(op),
      w.depth || 0,
    ]);
  }

  await new Promise((resolve, reject) => {
    content.on('data', (chunk) => {
      buffer += chunk;
      let newlineIdx;
      while ((newlineIdx = buffer.indexOf('\n')) !== -1) {
        let line = buffer.substring(0, newlineIdx).trim();
        buffer = buffer.substring(newlineIdx + 1);

        if (line === '[' || line === ']') continue;
        if (line.endsWith(',')) line = line.substring(0, line.length - 1);
        if (!line.startsWith('{')) continue;

        try { processWell(JSON.parse(line)); } catch (e) {}
      }
    });

    content.on('end', () => {
      let line = buffer.trim();
      if (line === ']' || line === '[' || !line) { resolve(); return; }
      if (line.endsWith(',')) line = line.substring(0, line.length - 1);
      if (line.startsWith('{')) {
        try { processWell(JSON.parse(line)); } catch (e) {}
      }
      resolve();
    });

    content.on('error', reject);
  });

  console.log(`\n  Total read: ${totalRead.toLocaleString()}`);
  console.log(`  Producing wells kept: ${totalKept.toLocaleString()}`);

  return {
    wells, opList, cntyList,
    countyMap, operatorMap, statusMap, typeMap, fieldMap,
    activeCountyMap, activeOperatorMap,
    totalRead, totalKept,
  };
}

async function main() {
  console.log('=== MineralSearch Site Data Builder ===\n');

  const result = await streamWellbores();

  const {
    wells, opList, cntyList,
    countyMap, operatorMap, statusMap, typeMap, fieldMap,
    activeCountyMap, activeOperatorMap,
    totalRead, totalKept,
  } = result;

  // Active wells count (all active statuses, not just producing)
  let activeTotal = 0;
  for (const st of ['PRODUCING', 'SHUT IN', 'SHUT IN-MULTI-COMPL', 'INJECTION', 'PROD FACTOR WELL']) {
    activeTotal += (statusMap[st] || 0);
  }

  // Top 100 operators by active well count
  const topOperators = Object.entries(activeOperatorMap)
    .filter(([name]) => name.trim())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 100)
    .map(([name, count]) => ({ n: name, c: count }));

  // County breakdown (active wells only), sorted by count
  const counties = Object.entries(activeCountyMap)
    .sort((a, b) => b[1] - a[1])
    .map(([name, count]) => ({ n: name, c: count }));

  // Status breakdown (all wells)
  const statuses = Object.entries(statusMap)
    .filter(([name]) => name && name !== '(blank)')
    .sort((a, b) => b[1] - a[1])
    .map(([name, count]) => ({ n: name, c: count }));

  // Type breakdown
  const types = Object.entries(typeMap)
    .sort((a, b) => b[1] - a[1])
    .map(([name, count]) => ({ n: name || 'Unknown', c: count }));

  // Top 50 fields
  const topFields = Object.entries(fieldMap)
    .filter(([name]) => name.trim())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 50)
    .map(([name, count]) => ({ n: name, c: count }));

  // Assemble output
  const siteData = {
    meta: {
      generatedAt: new Date().toISOString(),
      source: 'Texas Railroad Commission (RRC)',
      // Document the tuple format so site.js can decode it
      wellTuple: ['api', 'countyId', 'type', 'opId', 'depth'],
    },
    stats: {
      totalWells: totalRead,
      activeWells: activeTotal,
      producingWells: statusMap['PRODUCING'] || 0,
      shutInWells: (statusMap['SHUT IN'] || 0) + (statusMap['SHUT IN-MULTI-COMPL'] || 0),
      injectionWells: statusMap['INJECTION'] || 0,
      counties: Object.keys(countyMap).length,
      activeCounties: counties.length,
      operators: Object.keys(operatorMap).length,
    },
    // Lookup tables for interned strings
    opList,    // operator index -> name
    cntyList,  // county index -> name
    topOperators,
    counties,
    statuses,
    types,
    topFields,
    wells,
  };

  console.log('\nWriting site-data.json...');
  const json = JSON.stringify(siteData);
  fs.writeFileSync(OUTPUT_FILE, json, 'utf8');

  const sizeMB = (Buffer.byteLength(json, 'utf8') / (1024 * 1024)).toFixed(1);
  console.log(`  Output size: ${sizeMB} MB`);
  console.log(`  Wells in output: ${wells.length.toLocaleString()}`);
  console.log(`  Top operator: ${topOperators[0]?.n} (${topOperators[0]?.c} active wells)`);
  console.log(`  Active counties: ${counties.length}`);
  console.log(`  Operator lookup entries: ${opList.length}`);
  console.log(`  County lookup entries: ${cntyList.length}`);
  console.log('\nDone!');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
