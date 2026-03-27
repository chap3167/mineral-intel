#!/usr/bin/env node
/**
 * build-database.js
 * Combine all scraped data into a master cross-referenced database.
 */

const fs = require('fs');
const path = require('path');

const DATA_DIR = path.join(__dirname, 'data');

function loadJSON(filename) {
  const filepath = path.join(DATA_DIR, filename);
  if (!fs.existsSync(filepath)) {
    console.log(`  Warning: ${filename} not found, skipping.`);
    return [];
  }
  const data = JSON.parse(fs.readFileSync(filepath, 'utf8'));
  console.log(`  Loaded ${data.length} records from ${filename}`);
  return data;
}

function normalizeApiNumber(api) {
  if (!api) return null;
  return api.replace(/[^0-9]/g, '').substring(0, 10);
}

function main() {
  console.log('=== Building Master Database ===\n');
  console.log('Loading data sources...');

  const permits = loadJSON('rrc-permits.json');
  const production = loadJSON('rrc-production.json');
  const completions = loadJSON('rrc-completions.json');
  const clerkRecords = loadJSON('county-clerk-records.json');
  const cadMinerals = loadJSON('cad-minerals.json');
  const gloLeases = loadJSON('glo-leases.json');
  const fracData = loadJSON('fracfocus.json');

  console.log('\nBuilding well index...');

  // Create a master well index keyed by API number
  const wellIndex = new Map();

  // Index production wells (primary source - has the most data)
  production.forEach(w => {
    const key = normalizeApiNumber(w.apiNumber);
    if (!key) return;
    wellIndex.set(key, {
      apiNumber: w.apiNumber,
      operator: w.operator,
      leaseName: w.leaseName,
      wellNumber: w.wellNumber,
      county: w.county,
      district: w.district,
      wellType: w.wellType,
      status: 'Producing',
      permitDate: null,
      completionDate: null,
      formation: null,
      totalDepth: null,
      lateralLength: null,
      ipOilBblDay: null,
      ipGasMcfDay: null,
      production: w.production || [],
      totalOil: w.totalOil || 0,
      totalGas: w.totalGas || 0,
      ownership: [],
      leases: [],
      fracData: null
    });
  });

  console.log(`  Indexed ${wellIndex.size} wells from production data`);

  // Merge completion data
  let completionMatches = 0;
  completions.forEach(c => {
    const key = normalizeApiNumber(c.apiNumber);
    if (!key) return;

    if (wellIndex.has(key)) {
      const well = wellIndex.get(key);
      well.completionDate = c.completionDate;
      well.formation = c.formation;
      well.totalDepth = c.totalDepth;
      well.lateralLength = c.lateralLength;
      well.ipOilBblDay = c.ipOilBblDay;
      well.ipGasMcfDay = c.ipGasMcfDay;
      completionMatches++;
    } else {
      // Add wells that only appear in completions
      wellIndex.set(key, {
        apiNumber: c.apiNumber,
        operator: c.operator,
        leaseName: c.leaseName,
        wellNumber: c.wellNumber,
        county: c.county,
        district: c.district,
        wellType: c.wellType,
        status: 'Completed',
        permitDate: null,
        completionDate: c.completionDate,
        formation: c.formation,
        totalDepth: c.totalDepth,
        lateralLength: c.lateralLength,
        ipOilBblDay: c.ipOilBblDay,
        ipGasMcfDay: c.ipGasMcfDay,
        production: [],
        totalOil: 0,
        totalGas: 0,
        ownership: [],
        leases: [],
        fracData: null
      });
    }
  });
  console.log(`  Merged ${completionMatches} completion records`);

  // Merge permit data
  let permitMatches = 0;
  permits.forEach(p => {
    const key = normalizeApiNumber(p.apiNumber);
    if (!key) return;

    if (wellIndex.has(key)) {
      const well = wellIndex.get(key);
      well.permitDate = p.permitDate;
      if (!well.totalDepth && p.proposedDepth) well.totalDepth = p.proposedDepth;
      permitMatches++;
    } else {
      wellIndex.set(key, {
        apiNumber: p.apiNumber,
        operator: p.operator,
        leaseName: p.leaseName,
        wellNumber: p.wellNumber,
        county: p.county,
        district: p.district,
        wellType: p.wellType,
        status: p.status === 'Approved' ? 'Permitted' : p.status,
        permitDate: p.permitDate,
        completionDate: null,
        formation: null,
        totalDepth: p.proposedDepth,
        lateralLength: null,
        ipOilBblDay: null,
        ipGasMcfDay: null,
        production: [],
        totalOil: 0,
        totalGas: 0,
        ownership: [],
        leases: [],
        fracData: null
      });
    }
  });
  console.log(`  Merged ${permitMatches} permit records`);

  // Merge FracFocus data
  let fracMatches = 0;
  fracData.forEach(f => {
    const key = normalizeApiNumber(f.apiNumber);
    if (!key) return;

    if (wellIndex.has(key)) {
      const well = wellIndex.get(key);
      well.fracData = {
        fractureDate: f.fractureDate,
        totalWaterVolumeGallons: f.totalWaterVolumeGallons,
        totalWaterVolumeBbls: f.totalWaterVolumeBbls,
        numberOfStages: f.numberOfStages,
        proppantLbs: f.proppantLbs,
        proppantType: f.proppantType,
        chemicalCount: f.chemicals ? f.chemicals.length : 0,
        chemicals: f.chemicals || []
      };
      fracMatches++;
    }
  });
  console.log(`  Merged ${fracMatches} frac disclosure records`);

  // NOTE: Deed/lease cross-referencing removed.
  // Previously this code randomly assigned deeds and leases to wells using Math.random(),
  // creating fake linkages. Without real API-number-to-deed matching, we cannot create
  // accurate ownership/lease associations. Wells will have empty ownership[] and leases[]
  // arrays until real cross-reference data is available.

  const deedTypes = ['MINERAL DEED', 'ROYALTY DEED', 'MINERAL INTEREST CONVEYANCE'];
  const deeds = clerkRecords.filter(r => deedTypes.includes(r.documentType));
  const leaseRecords = clerkRecords.filter(r => r.documentType === 'OIL AND GAS LEASE');

  console.log(`  Found ${deeds.length} mineral deeds (not linked — no real cross-reference available)`);
  console.log(`  Found ${leaseRecords.length} lease records (not linked — no real cross-reference available)`);
  console.log(`  Found ${gloLeases.length} GLO leases (not linked — no real cross-reference available)`);

  // Convert to array and sort
  const masterDB = Array.from(wellIndex.values());
  masterDB.sort((a, b) => b.totalOil - a.totalOil);

  // Generate stats
  const stats = {
    totalWells: masterDB.length,
    producingWells: masterDB.filter(w => w.status === 'Producing').length,
    permittedWells: masterDB.filter(w => w.status === 'Permitted').length,
    completedWells: masterDB.filter(w => w.status === 'Completed').length,
    counties: [...new Set(masterDB.map(w => w.county))].length,
    operators: [...new Set(masterDB.map(w => w.operator))].length,
    totalOilBbls: masterDB.reduce((s, w) => s + (w.totalOil || 0), 0),
    totalGasMcf: masterDB.reduce((s, w) => s + (w.totalGas || 0), 0),
    wellsWithProduction: masterDB.filter(w => w.production && w.production.length > 0).length,
    wellsWithOwnership: masterDB.filter(w => w.ownership && w.ownership.length > 0).length,
    wellsWithLeases: masterDB.filter(w => w.leases && w.leases.length > 0).length,
    wellsWithFracData: masterDB.filter(w => w.fracData).length,
    totalLeaseRecords: leaseRecords.length,
    totalDeedRecords: deeds.length,
    totalCadAccounts: loadJSON('cad-minerals.json').length,
    buildDate: new Date().toISOString()
  };

  console.log('\n=== Master Database Stats ===');
  Object.entries(stats).forEach(([k, v]) => console.log(`  ${k}: ${v}`));

  // Save master database
  const output = { stats, wells: masterDB };
  fs.writeFileSync(path.join(DATA_DIR, 'master-database.json'), JSON.stringify(output, null, 2));
  console.log(`\nSaved master-database.json (${masterDB.length} wells)`);

  // Generate CSV
  const csvHeaders = [
    'API Number', 'Operator', 'Lease Name', 'Well Number', 'County', 'District',
    'Well Type', 'Status', 'Permit Date', 'Completion Date', 'Formation',
    'Total Depth', 'Lateral Length', 'IP Oil (bbl/day)', 'IP Gas (mcf/day)',
    'Total Oil (12mo)', 'Total Gas (12mo)', 'Ownership Count', 'Lease Count',
    'Has Frac Data'
  ];

  const csvRows = masterDB.map(w => [
    w.apiNumber, `"${w.operator}"`, `"${w.leaseName}"`, w.wellNumber,
    w.county, w.district, w.wellType, w.status,
    w.permitDate || '', w.completionDate || '', w.formation || '',
    w.totalDepth || '', w.lateralLength || '',
    w.ipOilBblDay || '', w.ipGasMcfDay || '',
    w.totalOil, w.totalGas,
    w.ownership ? w.ownership.length : 0,
    w.leases ? w.leases.length : 0,
    w.fracData ? 'Yes' : 'No'
  ].join(','));

  const csv = [csvHeaders.join(','), ...csvRows].join('\n');
  fs.writeFileSync(path.join(DATA_DIR, 'master-database.csv'), csv);
  console.log(`Saved master-database.csv`);

  // Also save standalone ownership, leases, and CAD data in the master output
  const supplementary = {
    clerkRecords: clerkRecords.length,
    cadMinerals: loadJSON('cad-minerals.json').length,
    gloLeases: gloLeases.length,
    fracDisclosures: fracData.length
  };
  console.log('\nSupplementary data counts:', supplementary);
  console.log('\n=== Build Complete ===');
}

main();
