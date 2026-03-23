/**
 * Parse OG_WELLBORE_EWA_Report.csv into our database format.
 * 1.3M wells — stream-reads to avoid memory issues.
 */

const fs = require('fs');
const readline = require('readline');
const path = require('path');

const INPUT = path.join(__dirname, 'data', 'raw', 'OG_WELLBORE_EWA_Report.csv');
const OUTPUT = path.join(__dirname, 'data', 'parsed-wellbores.json');
const OUTPUT_SUMMARY = path.join(__dirname, 'data', 'wellbore-summary.json');

async function parse() {
  console.log('=== Parsing OG_WELLBORE_EWA_Report.csv ===');
  console.log(`Input: ${INPUT}`);
  console.log(`File size: ${(fs.statSync(INPUT).size / 1024 / 1024).toFixed(1)} MB`);

  const rl = readline.createInterface({
    input: fs.createReadStream(INPUT),
    crlfDelay: Infinity,
  });

  const wells = [];
  const counties = {};
  const operators = {};
  const statuses = {};
  let lineNum = 0;
  let errors = 0;

  for await (const line of rl) {
    lineNum++;
    if (lineNum % 100000 === 0) console.log(`  Processed ${lineNum.toLocaleString()} lines...`);

    try {
      // CSV with quoted fields
      const fields = line.match(/(".*?"|[^",]+)/g);
      if (!fields || fields.length < 20) continue;

      // Clean quotes
      const clean = fields.map(f => f.replace(/^"|"$/g, '').trim());

      const district = clean[0];
      const countyCode = clean[1];
      const apiSuffix = clean[2];
      const county = clean[3];
      const wellType = clean[4]; // O=oil, G=gas
      const leaseName = clean[5];
      const fieldNumber = clean[6];
      const fieldName = clean[7];
      const wellNumber = clean[9];
      const operator = clean[11];
      const operatorNumber = clean[12];
      const wellboreProfile = clean[13];
      const totalDepth = parseInt(clean[15]) || 0;
      const status = clean[18];
      const apiNumber = `42-${countyCode.padStart(3, '0')}-${apiSuffix}`;

      // Track stats
      counties[county] = (counties[county] || 0) + 1;
      operators[operator] = (operators[operator] || 0) + 1;
      statuses[status] = (statuses[status] || 0) + 1;

      // Only store essential fields to manage file size
      // For the full site, we'd use a real database
      wells.push({
        api: apiNumber,
        dist: district,
        county,
        type: wellType,
        lease: leaseName,
        field: fieldName,
        well: wellNumber,
        op: operator,
        opNum: operatorNumber,
        profile: wellboreProfile,
        depth: totalDepth,
        status,
      });

    } catch (e) {
      errors++;
    }
  }

  console.log(`\n=== Parse Complete ===`);
  console.log(`Total lines: ${lineNum.toLocaleString()}`);
  console.log(`Wells parsed: ${wells.length.toLocaleString()}`);
  console.log(`Errors: ${errors}`);
  console.log(`Unique counties: ${Object.keys(counties).length}`);
  console.log(`Unique operators: ${Object.keys(operators).length}`);

  // Top 20 counties
  console.log('\nTop 20 counties by well count:');
  Object.entries(counties).sort((a, b) => b[1] - a[1]).slice(0, 20).forEach(([c, n]) => {
    console.log(`  ${c}: ${n.toLocaleString()}`);
  });

  // Status breakdown
  console.log('\nWell statuses:');
  Object.entries(statuses).sort((a, b) => b[1] - a[1]).forEach(([s, n]) => {
    console.log(`  ${s}: ${n.toLocaleString()}`);
  });

  // Save summary (small file for the website)
  const summary = {
    totalWells: wells.length,
    counties: Object.entries(counties).sort((a, b) => b[1] - a[1]).map(([name, count]) => ({ name, count })),
    operators: Object.entries(operators).sort((a, b) => b[1] - a[1]).slice(0, 500).map(([name, count]) => ({ name, count })),
    statuses: Object.entries(statuses).sort((a, b) => b[1] - a[1]).map(([name, count]) => ({ name, count })),
    parsedAt: new Date().toISOString(),
  };

  fs.writeFileSync(OUTPUT_SUMMARY, JSON.stringify(summary, null, 2));
  console.log(`\nSummary saved: ${OUTPUT_SUMMARY}`);

  // Save wells — this will be large, but manageable as JSON
  // For a real production system you'd use SQLite or PostgreSQL
  console.log(`Saving ${wells.length.toLocaleString()} wells to ${OUTPUT}...`);

  // Write as newline-delimited JSON to manage memory
  const ws = fs.createWriteStream(OUTPUT);
  ws.write('[\n');
  for (let i = 0; i < wells.length; i++) {
    ws.write(JSON.stringify(wells[i]));
    if (i < wells.length - 1) ws.write(',\n');
    else ws.write('\n');
  }
  ws.write(']\n');
  ws.end();

  console.log('Done!');
}

parse().catch(console.error);
