-- Lease-level monthly production data from RRC
-- Run this in Supabase SQL Editor before uploading data

CREATE TABLE IF NOT EXISTS lease_production (
  id BIGSERIAL PRIMARY KEY,
  oil_gas_code TEXT,
  district_no TEXT,
  lease_no TEXT,
  cycle_year INTEGER,
  cycle_month INTEGER,
  cycle_year_month TEXT,
  operator_no TEXT,
  field_no TEXT,
  lease_name TEXT,
  operator_name TEXT,
  field_name TEXT,
  oil_prod_vol BIGINT,
  gas_prod_vol BIGINT,
  cond_prod_vol BIGINT,
  csgd_prod_vol BIGINT
);

-- Indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_lp_lease ON lease_production(district_no, lease_no);
CREATE INDEX IF NOT EXISTS idx_lp_operator ON lease_production(operator_name);
CREATE INDEX IF NOT EXISTS idx_lp_ym ON lease_production(cycle_year_month);
CREATE INDEX IF NOT EXISTS idx_lp_year ON lease_production(cycle_year);
CREATE INDEX IF NOT EXISTS idx_lp_field ON lease_production(field_name);

-- Enable public read access
ALTER TABLE lease_production ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow public read" ON lease_production FOR SELECT USING (true);
