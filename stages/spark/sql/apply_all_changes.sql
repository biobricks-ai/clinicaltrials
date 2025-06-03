-- Apply all diff changes to Iceberg table using MERGE INTO for atomicity

-- Create temporary view from upserts parquet
CREATE OR REPLACE TEMPORARY VIEW upserts_data
USING parquet
OPTIONS (path "brick/diff/upserts.parquet");

-- @@

-- Create temporary view from deletions parquet
CREATE OR REPLACE TEMPORARY VIEW deletions_data
USING parquet
OPTIONS (path "brick/diff/deletions.parquet");

-- @@

-- Apply all changes (deletes, inserts, updates) using single MERGE INTO
MERGE INTO local.ctg_studies AS target
USING (
  SELECT nct_id, core_study_record, derived_section_norm_json, version_holder_json, columns_to_update, 'UPSERT' as operation
  FROM upserts_data
  UNION ALL
  SELECT nct_id, NULL as core_study_record, NULL as derived_section_norm_json, NULL as version_holder_json, NULL as columns_to_update, 'DELETE' as operation
  FROM deletions_data
) AS source
ON target.nct_id = source.nct_id
WHEN MATCHED AND source.operation = 'DELETE' THEN DELETE
WHEN MATCHED AND source.operation = 'UPSERT' THEN UPDATE SET
  core_study_record = CASE
    WHEN array_contains(source.columns_to_update, 'core_study_record')
    THEN source.core_study_record
    ELSE target.core_study_record
  END,
  derived_section_norm_json = CASE
    WHEN array_contains(source.columns_to_update, 'derived_section_norm_json')
    THEN source.derived_section_norm_json
    ELSE target.derived_section_norm_json
  END,
  version_holder_json = CASE
    WHEN array_contains(source.columns_to_update, 'version_holder_json')
    THEN source.version_holder_json
    ELSE target.version_holder_json
  END
WHEN NOT MATCHED AND source.operation = 'UPSERT' THEN INSERT (
  nct_id,
  core_study_record,
  derived_section_norm_json,
  version_holder_json
) VALUES (
  source.nct_id,
  source.core_study_record,
  source.derived_section_norm_json,
  source.version_holder_json
);

-- @@

-- Show summary of changes
SELECT
    operation_type,
    COUNT(*) as record_count
FROM upserts_data
GROUP BY operation_type

UNION ALL

SELECT
    'DELETIONS' as operation_type,
    COUNT(*) as record_count
FROM deletions_data;
