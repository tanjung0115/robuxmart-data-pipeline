-- ============================================================================
-- 01_init_schemas.sql
-- Initialize Data Warehouse Multi-Layer Architecture (Medallion / Kimball)
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS marts;

COMMENT ON SCHEMA staging IS 'Raw & cleaned staging area extracted from RobuxMart OLTP (MySQL/Files)';
COMMENT ON SCHEMA warehouse IS 'Dimensional Star Schema (Fact & Dimension tables) with PII masking';
COMMENT ON SCHEMA marts IS 'Aggregated business & security Data Marts for BI and Data Analysts';
