#!/usr/bin/env node
/**
 * build-site-data-v2.js
 * Combines onshore Texas wells (from parsed-wellbores.json) with
 * offshore Gulf of Mexico wells (from gom-wells.json) into a single
 * site-data.json for the MineralSearch platform.
 *
 * Reads onshore wells via streaming (268MB file), offshore wells in bulk.
 * Marks each well as "onshore" or "offshore".
 * Target output: under 15MB.
 */

const fs = require('fs');
const path = require('path');

const DATA_DIR = path.join(__dirname, 'data');
const WELLBORE_FILE = path.join(DATA_DIR, 'parsed-wellbores.json');
const GOM_WELLS_FILE = path.join(DATA_DIR, 'gom-wells.json');
const GOM_SUMMARY_FILE = path.join(DATA_DIR, 'gom-summary.json');
const OUTPUT_FILE = path.join(DATA_DIR, 'site-data.json');

// Only PRODUCING wells go into the browsable onshore array
const INCLUDE_ONSHORE = new Set(['PRODUCING']);
// Offshore statuses to include in browsable array
const INCLUDE_OFFSHORE = new Set(['COM', 'PA', 'ST', 'TA']);
// But limit offshore to active-ish wells to keep size down
const OFFSHORE_ACTIVE = new Set(['COM']);

// All "active" statuses for onshore stats
const ACTIVE_STATUSES = new Set([
  'PRODUCING', 'SHUT IN', 'SHUT IN-MULTI-COMPL', 'INJECTION', 'PROD FACTOR WELL',
]);

async function streamOnshoreWells() {
  console.log('Streaming onshore wells from', WELLBORE_FILE, '...');

  const wells = [];
  const countyMap = {};
  const operatorMap = {};
  const statusMap = {};
  const typeMap = {};
  const fieldMap = {};
  const activeCountyMap = {};
  const activeOperatorMap = {};

  let totalRead = 0;
  let totalKept = 0;

  // String interning for operators
  const opIndex = {};
  const opList = [];
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
      process.stdout.write(`  Read ${(totalRead / 1000).toFixed(0)}K onshore wells, kept ${(totalKept / 1000).toFixed(0)}K...\r`);
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

    if (ACTIVE_STATUSES.has(st)) {
      if (county) activeCountyMap[county] = (activeCountyMap[county] || 0) + 1;
      if (op) activeOperatorMap[op] = (activeOperatorMap[op] || 0) + 1;
    }

    if (!INCLUDE_ONSHORE.has(st)) return;

    totalKept++;

    const api = (w.api || '').replace(/^42-/, '');
    // Tuple: [api, countyId, type, opId, depth, source]
    // source: 0 = onshore
    wells.push([
      api,
      getCntyId(county),
      wellType,
      getOpId(op),
      w.depth || 0,
      0,  // onshore marker
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

  console.log(`\n  Total onshore read: ${totalRead.toLocaleString()}`);
  console.log(`  Onshore producing kept: ${totalKept.toLocaleString()}`);

  return {
    wells, opList, opIndex, cntyList, cntyIndex, getOpId, getCntyId,
    countyMap, operatorMap, statusMap, typeMap, fieldMap,
    activeCountyMap, activeOperatorMap,
    totalRead, totalKept,
  };
}

function loadOffshoreWells(onshoreResult) {
  console.log('\nLoading offshore wells from', GOM_WELLS_FILE, '...');

  const gomWells = JSON.parse(fs.readFileSync(GOM_WELLS_FILE, 'utf8'));
  console.log(`  Total offshore wells: ${gomWells.length.toLocaleString()}`);

  const { wells, getOpId, getCntyId, statusMap } = onshoreResult;
  let offshoreTotal = 0;
  let offshoreKept = 0;

  // Offshore status map for combined stats
  const offshoreStatusMap = {};

  // BSEE status code meanings
  const statusLabels = {
    'COM': 'COMPLETED',
    'PA': 'PLUGGED & ABANDONED',
    'ST': 'SIDETRACK',
    'TA': 'TEMPORARILY ABANDONED',
    'DSI': 'DRILLING SUSPENDED',
    'DRL': 'DRILLING',
    'APD': 'APPLICATION FOR PERMIT',
    'CNL': 'CANCELLED',
  };

  for (const w of gomWells) {
    offshoreTotal++;
    const st = w.status || '';
    offshoreStatusMap[st] = (offshoreStatusMap[st] || 0) + 1;

    // Only include completed (active) wells in browsable array to keep size manageable
    if (!OFFSHORE_ACTIVE.has(st)) continue;

    offshoreKept++;

    // For offshore, use area/block as the "county" equivalent
    const area = w.surfaceAreaBlock || w.areaBlock || '';
    const opCode = w.operatorCode || '';

    wells.push([
      w.api,
      getCntyId(area),
      w.wellClass === 'E' ? 'EXPLORATORY' : w.wellClass === 'D' ? 'DEVELOPMENT' : w.wellClass || '',
      getOpId(opCode),
      w.totalDepthMD || 0,
      1,  // offshore marker
    ]);
  }

  console.log(`  Offshore completed (added to array): ${offshoreKept.toLocaleString()}`);
  console.log(`  Offshore status breakdown:`);
  for (const [st, count] of Object.entries(offshoreStatusMap).sort((a, b) => b[1] - a[1])) {
    console.log(`    ${st} (${statusLabels[st] || st}): ${count.toLocaleString()}`);
  }

  return { offshoreTotal, offshoreKept, offshoreStatusMap };
}

async function main() {
  console.log('=== MineralSearch Site Data Builder v2 (Onshore + Offshore) ===\n');

  // Step 1: Stream onshore wells
  const onshore = await streamOnshoreWells();

  // Step 2: Load offshore wells and merge into the same arrays
  const offshore = loadOffshoreWells(onshore);

  // Step 3: Load GOM summary for additional stats
  let gomSummary = {};
  if (fs.existsSync(GOM_SUMMARY_FILE)) {
    gomSummary = JSON.parse(fs.readFileSync(GOM_SUMMARY_FILE, 'utf8'));
  }

  const {
    wells, opList, cntyList,
    countyMap, operatorMap, statusMap, typeMap, fieldMap,
    activeCountyMap, activeOperatorMap,
    totalRead: onshoreTotal, totalKept: onshoreKept,
  } = onshore;

  // Active wells count (onshore only - offshore has different status codes)
  let activeOnshore = 0;
  for (const st of ['PRODUCING', 'SHUT IN', 'SHUT IN-MULTI-COMPL', 'INJECTION', 'PROD FACTOR WELL']) {
    activeOnshore += (statusMap[st] || 0);
  }

  // Top 100 operators by active well count (onshore)
  const topOperators = Object.entries(activeOperatorMap)
    .filter(([name]) => name.trim())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 100)
    .map(([name, count]) => ({ n: name, c: count }));

  // County breakdown (onshore active wells only)
  const counties = Object.entries(activeCountyMap)
    .sort((a, b) => b[1] - a[1])
    .map(([name, count]) => ({ n: name, c: count }));

  // Status breakdown (onshore)
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
      sources: [
        'Texas Railroad Commission (RRC)',
        'BSEE (Bureau of Safety and Environmental Enforcement)',
      ],
      // Document the tuple format so site.js can decode it
      wellTuple: ['api', 'countyId', 'type', 'opId', 'depth', 'source'],
      sourceValues: { 0: 'onshore', 1: 'offshore' },
    },
    stats: {
      totalWells: onshoreTotal + offshore.offshoreTotal,
      onshoreWells: onshoreTotal,
      offshoreWells: offshore.offshoreTotal,
      activeWells: activeOnshore + (offshore.offshoreStatusMap['COM'] || 0),
      producingWells: statusMap['PRODUCING'] || 0,
      shutInWells: (statusMap['SHUT IN'] || 0) + (statusMap['SHUT IN-MULTI-COMPL'] || 0),
      injectionWells: statusMap['INJECTION'] || 0,
      offshoreCompleted: offshore.offshoreStatusMap['COM'] || 0,
      offshorePA: offshore.offshoreStatusMap['PA'] || 0,
      counties: Object.keys(countyMap).length,
      activeCounties: counties.length,
      operators: Object.keys(operatorMap).length,
    },
    offshore: {
      totalWells: offshore.offshoreTotal,
      wellsInArray: offshore.offshoreKept,
      statuses: offshore.offshoreStatusMap,
      topOperators: gomSummary.wells?.topOperators?.slice(0, 20) || [],
      topAreas: gomSummary.wells?.topAreas?.slice(0, 20) || [],
      avgWaterDepthFt: gomSummary.wells?.avgWaterDepthFt || 0,
      avgTotalDepthFt: gomSummary.wells?.avgTotalDepthFt || 0,
    },
    // Lookup tables for interned strings
    opList,
    cntyList,
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
  console.log(`  Wells in output array: ${wells.length.toLocaleString()}`);

  if (parseFloat(sizeMB) > 15) {
    console.warn(`\n  WARNING: Output exceeds 15MB target (${sizeMB} MB).`);
    console.warn('  Consider filtering more aggressively or using compact encoding.');
  } else {
    console.log(`  Size is within 15MB target.`);
  }

  console.log(`\n  Onshore wells: ${onshoreTotal.toLocaleString()} total, ${onshoreKept.toLocaleString()} producing`);
  console.log(`  Offshore wells: ${offshore.offshoreTotal.toLocaleString()} total, ${offshore.offshoreKept.toLocaleString()} completed`);
  console.log(`  Combined total: ${(onshoreTotal + offshore.offshoreTotal).toLocaleString()}`);
  console.log(`  Top operator: ${topOperators[0]?.n} (${topOperators[0]?.c} active wells)`);
  console.log(`  Operator lookup entries: ${opList.length}`);
  console.log(`  County/area lookup entries: ${cntyList.length}`);
  console.log('\nDone!');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
