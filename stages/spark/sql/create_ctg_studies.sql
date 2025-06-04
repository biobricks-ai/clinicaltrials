-- Create two-table schema for clinical trials data
-- Separates core study data from frequently-changing derived metadata

-- Core data table: Contains stable clinical trial information
CREATE TABLE IF NOT EXISTS local.ctg_studies_core (
  nct_id STRING,
  core_study_record STRING
)
USING iceberg
TBLPROPERTIES (
  'write.merge.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.delete.mode' = 'merge-on-read',
  'write.sort-order' = 'nct_id'
);

-- @@

-- Derived data table: Contains frequently-updated metadata
CREATE TABLE IF NOT EXISTS local.ctg_studies_derived (
  nct_id STRING,
  derived_section_norm_json STRING
)
USING iceberg
TBLPROPERTIES (
  'write.merge.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.delete.mode' = 'merge-on-read',
  'write.sort-order' = 'nct_id'
);
