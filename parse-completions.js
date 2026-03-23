/**
 * Parse OG_WELL_COMPLETION_DATA_TABLE.dsv from the PDQ zip.
 * Extract just this one file without unzipping the whole 34GB.
 */

const { execSync } = require('child_process');
const fs = require('fs');
const readline = require('readline');
const path = require('path');

const ZIP_FILE = path.join(__dirname, 'data', 'raw', 'PDQ_DSV.zip');
const EXTRACT_DIR = path.join(__dirname, 'data', 'raw', 'pdq_extracted');
const OUTPUT = path.join(__dirname, 'data', 'parsed-completions.json');

async function parse() {
  console.log('=== Parsing Well Completion Data ===');

  // Extract only the completion file from the zip (58MB, manageable)
  if (!fs.existsSync(EXTRACT_DIR)) fs.mkdirSync(EXTRACT_DIR, { recursive: true });

  console.log('Extracting OG_WELL_COMPLETION_DATA_TABLE.dsv from PDQ zip...');
  try {
    execSync(`unzip -o "${ZIP_FILE}" OG_WELL_COMPLETION_DATA_TABLE.dsv -d "${EXTRACT_DIR}"`, { stdio: 'pipe' });
  } catch (e) {
    console.log('Could not extract. Trying OG_SUMMARY_MASTER_LARGE_DATA_TABLE.dsv instead...');
    try {
      execSync(`unzip -o "${ZIP_FILE}" OG_SUMMARY_MASTER_LARGE_DATA_TABLE.dsv -d "${EXTRACT_DIR}"`, { stdio: 'pipe' });
    } catch (e2) {
      console.log('Extraction failed. Listing zip contents...');
      const list = execSync(`unzip -l "${ZIP_FILE}" | head -30`).toString();
      console.log(list);
      return;
    }
  }

  // Parse the completion file
  const completionFile = path.join(EXTRACT_DIR, 'OG_WELL_COMPLETION_DATA_TABLE.dsv');
  const summaryFile = path.join(EXTRACT_DIR, 'OG_SUMMARY_MASTER_LARGE_DATA_TABLE.dsv');

  let inputFile = completionFile;
  if (!fs.existsSync(completionFile) && fs.existsSync(summaryFile)) {
    inputFile = summaryFile;
  }

  if (!fs.existsSync(inputFile)) {
    console.log('No parseable file found.');
    return;
  }

  console.log(`Parsing: ${inputFile}`);
  console.log(`File size: ${(fs.statSync(inputFile).size / 1024 / 1024).toFixed(1)} MB`);

  const rl = readline.createInterface({
    input: fs.createReadStream(inputFile),
    crlfDelay: Infinity,
  });

  let headers = null;
  const records = [];
  let lineNum = 0;

  for await (const line of rl) {
    lineNum++;

    // DSV files use } as delimiter based on the header
    let fields;
    if (line.includes('}')) {
      fields = line.split('}');
    } else if (line.includes('|')) {
      fields = line.split('|');
    } else if (line.includes('\t')) {
      fields = line.split('\t');
    } else {
      fields = line.split(',');
    }

    if (lineNum === 1) {
      headers = fields.map(f => f.trim().replace(/"/g, ''));
      console.log(`Headers (${headers.length}): ${headers.slice(0, 10).join(', ')}...`);
      continue;
    }

    if (lineNum % 50000 === 0) console.log(`  Processed ${lineNum.toLocaleString()} lines...`);

    if (headers && fields.length >= 5) {
      const record = {};
      headers.forEach((h, i) => {
        if (fields[i]) record[h] = fields[i].trim().replace(/"/g, '');
      });
      records.push(record);
    }
  }

  console.log(`\nParsed ${records.length.toLocaleString()} completion records`);

  // Save
  fs.writeFileSync(OUTPUT, JSON.stringify(records.slice(0, 100000), null, 2)); // Cap at 100K for file size
  console.log(`Saved to ${OUTPUT}`);

  // Cleanup extracted file to save space
  try { fs.unlinkSync(inputFile); } catch(e) {}
  console.log('Done!');
}

parse().catch(console.error);
