#!/bin/bash
echo "============================================"
echo "  MineralIntel — Parse All RRC Data"
echo "  Started: $(date)"
echo "============================================"

cd "$(dirname "$0")"

echo ""
echo "[1/3] Parsing 1.3M wellbores..."
node parse-wellbores.js
echo ""

echo "[2/3] Parsing operators..."
node parse-operators.js
echo ""

echo "[3/3] Parsing completions from PDQ zip..."
node parse-completions.js
echo ""

echo "============================================"
echo "  Parse Complete: $(date)"
echo "============================================"
echo ""
echo "  Output files:"
ls -lh data/parsed-*.json data/wellbore-summary.json 2>/dev/null | awk '{print "    " $5 " " $9}'
echo ""
echo "  Free disk space:"
df -h / | awk 'NR==2 {print "    " $4 " free"}'
