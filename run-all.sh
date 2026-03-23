#!/bin/bash
# run-all.sh — Run all scrapers then build the master database.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "============================================"
echo "  MineralIntel — Full Data Pipeline"
echo "============================================"
echo ""

# Create data directory if needed
mkdir -p data

echo "[1/7] Scraping RRC drilling permits..."
node scrape-rrc-permits.js
echo ""

echo "[2/7] Scraping RRC production data..."
node scrape-rrc-production.js
echo ""

echo "[3/7] Scraping RRC well completions..."
node scrape-rrc-completions.js
echo ""

echo "[4/7] Scraping county clerk records..."
node scrape-county-clerks.js
echo ""

echo "[5/7] Scraping CAD mineral data..."
node scrape-cad.js
echo ""

echo "[6/7] Scraping GLO lease data..."
node scrape-glo.js
echo ""

echo "[7/7] Scraping FracFocus data..."
node scrape-fracfocus.js
echo ""

echo "============================================"
echo "  Building Master Database"
echo "============================================"
echo ""
node build-database.js

echo ""
echo "============================================"
echo "  Pipeline Complete!"
echo "============================================"
echo ""
echo "Data files:"
ls -lh data/*.json data/*.csv 2>/dev/null
echo ""
echo "Open index.html in a browser to view the platform."
