-- Apply changes to the core table using MERGE INTO

-- Create temporary view from upserts parquet (filtered for core changes)
CREATE OR REPLACE TEMPORARY VIEW core_upserts_data
AS SELECT
    nct_id,
    core_study_record,
    operation_type,
    columns_to_update
FROM parquet.`brick/diff/upserts.parquet`
WHERE array_contains(columns_to_update, 'core_study_record');

-- @@

-- Create temporary view from deletions parquet
CREATE OR REPLACE TEMPORARY VIEW core_deletions_data
AS SELECT
    nct_id
FROM parquet.`brick/diff/deletions.parquet`;

-- @@

-- Apply all changes (deletes, inserts, updates) using single MERGE INTO
MERGE INTO local.ctg_studies_core AS target
USING (
  SELECT
    nct_id,
    core_study_record,
    'UPSERT' as operation
  FROM core_upserts_data
  UNION ALL
  SELECT
    nct_id,
    NULL as core_study_record,
    'DELETE' as operation
  FROM core_deletions_data
) AS source
ON target.nct_id = source.nct_id
WHEN MATCHED AND source.operation = 'DELETE' THEN DELETE
WHEN MATCHED AND source.operation = 'UPSERT' THEN UPDATE SET
  core_study_record = source.core_study_record
WHEN NOT MATCHED AND source.operation = 'UPSERT' THEN INSERT (
  nct_id,
  core_study_record
) VALUES (
  source.nct_id,
  source.core_study_record
);

-- @@

-- Show summary of changes
SELECT
    'CORE TABLE CHANGES' as table_name,
    operation_type,
    COUNT(*) as record_count
FROM core_upserts_data
GROUP BY operation_type

UNION ALL

SELECT
    'CORE TABLE CHANGES' as table_name,
    'DELETIONS' as operation_type,
    COUNT(*) as record_count
FROM core_deletions_data;
