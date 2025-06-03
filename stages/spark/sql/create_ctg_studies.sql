CREATE TABLE IF NOT EXISTS local.ctg_studies (
  nct_id STRING,
  core_study_record STRING,
  derived_section_norm_json STRING,
  version_holder_json STRING
)
USING iceberg
TBLPROPERTIES (
  'write.merge.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.delete.mode' = 'merge-on-read'
);
