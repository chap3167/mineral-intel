/**
 * Parse orf850.txt — every operator registered in Texas.
 * Fixed-width format.
 */

const fs = require('fs');
const readline = require('readline');
const path = require('path');

const INPUT = path.join(__dirname, 'data', 'raw', 'orf850.txt');
const OUTPUT = path.join(__dirname, 'data', 'parsed-operators.json');

async function parse() {
  console.log('=== Parsing Operator Data (orf850.txt) ===');
  console.log(`File size: ${(fs.statSync(INPUT).size / 1024 / 1024).toFixed(1)} MB`);

  const rl = readline.createInterface({
    input: fs.createReadStream(INPUT),
    crlfDelay: Infinity,
  });

  const operators = [];
  let lineNum = 0;

  for await (const line of rl) {
    lineNum++;
    if (lineNum % 50000 === 0) console.log(`  Processed ${lineNum.toLocaleString()} lines...`);

    // Fixed-width format — the first character is record type
    const recType = line.substring(0, 1);

    // Try to extract operator info
    // Format varies but typically: type code + operator number + name + address
    if (line.length > 30) {
      const opNumber = line.substring(1, 7).trim();
      const opName = line.substring(7, 50).trim();
      const rest = line.substring(50).trim();

      if (opName && opName.length > 2 && !opName.match(/^[\d\s]+$/)) {
        operators.push({
          type: recType,
          number: opNumber,
          name: opName,
          info: rest.substring(0, 100),
        });
      }
    }
  }

  // Deduplicate by operator number
  const unique = {};
  operators.forEach(op => {
    if (!unique[op.number] || op.name.length > unique[op.number].name.length) {
      unique[op.number] = op;
    }
  });

  const deduped = Object.values(unique);

  console.log(`\nTotal lines: ${lineNum.toLocaleString()}`);
  console.log(`Operators found: ${operators.length.toLocaleString()}`);
  console.log(`Unique operators: ${deduped.length.toLocaleString()}`);

  fs.writeFileSync(OUTPUT, JSON.stringify(deduped, null, 2));
  console.log(`Saved to ${OUTPUT}`);
  console.log('Done!');
}

parse().catch(console.error);
