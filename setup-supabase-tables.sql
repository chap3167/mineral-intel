-- ============================================================
-- MineralSearch Platform — RRC Data Tables
-- Run this in the Supabase SQL Editor to create all tables.
-- ============================================================

-- 1. Production by county (monthly)
CREATE TABLE IF NOT EXISTS production_county (
  id SERIAL PRIMARY KEY,
  county_no TEXT,
  county_name TEXT,
  district_no TEXT,
  cycle_year INTEGER,
  cycle_month INTEGER,
  cycle_year_month TEXT,
  oil_prod_vol BIGINT,
  gas_prod_vol BIGINT,
  cond_prod_vol BIGINT,
  csgd_prod_vol BIGINT
);
CREATE INDEX IF NOT EXISTS idx_prod_county_name ON production_county(county_name);
CREATE INDEX IF NOT EXISTS idx_prod_county_ym ON production_county(cycle_year_month);
ALTER TABLE production_county ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow public read" ON production_county FOR SELECT USING (true);

-- 2. Operators
CREATE TABLE IF NOT EXISTS operators (
  id SERIAL PRIMARY KEY,
  operator_no TEXT UNIQUE,
  operator_name TEXT,
  status_code TEXT,
  last_filed_date TEXT
);
CREATE INDEX IF NOT EXISTS idx_operators_name ON operators(operator_name);
ALTER TABLE operators ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow public read" ON operators FOR SELECT USING (true);

-- 3. Lease summary (onshore)
CREATE TABLE IF NOT EXISTS leases (
  id SERIAL PRIMARY KEY,
  oil_gas_code TEXT,
  district_no TEXT,
  lease_no TEXT,
  operator_no TEXT,
  operator_name TEXT,
  field_no TEXT,
  field_name TEXT,
  lease_name TEXT,
  cycle_min TEXT,
  cycle_max TEXT
);
CREATE INDEX IF NOT EXISTS idx_leases_operator ON leases(operator_name);
CREATE INDEX IF NOT EXISTS idx_leases_field ON leases(field_name);
CREATE INDEX IF NOT EXISTS idx_leases_name ON leases(lease_name);
ALTER TABLE leases ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow public read" ON leases FOR SELECT USING (true);

-- 4. Regulatory lease details
CREATE TABLE IF NOT EXISTS lease_details (
  id SERIAL PRIMARY KEY,
  oil_gas_code TEXT,
  district_no TEXT,
  district_name TEXT,
  lease_no TEXT,
  lease_name TEXT,
  operator_no TEXT,
  operator_name TEXT,
  field_no TEXT,
  field_name TEXT,
  well_no TEXT
);
CREATE INDEX IF NOT EXISTS idx_lease_details_operator ON lease_details(operator_name);
CREATE INDEX IF NOT EXISTS idx_lease_details_lease ON lease_details(lease_name);
ALTER TABLE lease_details ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow public read" ON lease_details FOR SELECT USING (true);

-- 5. Fields
CREATE TABLE IF NOT EXISTS fields (
  id SERIAL PRIMARY KEY,
  field_no TEXT UNIQUE,
  field_name TEXT,
  district_no TEXT,
  district_name TEXT,
  field_class TEXT
);
CREATE INDEX IF NOT EXISTS idx_fields_name ON fields(field_name);
ALTER TABLE fields ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow public read" ON fields FOR SELECT USING (true);
