-- Apply changes to the derived table using MERGE INTO

-- Create temporary view from upserts parquet (filtered for derived changes)
CREATE OR REPLACE TEMPORARY VIEW derived_upserts_data
AS SELECT
    nct_id,
    derived_section_norm_json,
    operation_type,
    columns_to_update
FROM parquet.`brick/diff/upserts.parquet`
WHERE array_contains(columns_to_update, 'derived_section_norm_json');

-- @@

-- Create temporary view from deletions parquet
CREATE OR REPLACE TEMPORARY VIEW derived_deletions_data
AS SELECT
    nct_id
FROM parquet.`brick/diff/deletions.parquet`;

-- @@

-- Apply all changes (deletes, inserts, updates) using single MERGE INTO
MERGE INTO local.ctg_studies_derived AS target
USING (
  SELECT
    nct_id,
    derived_section_norm_json,
    'UPSERT' as operation
  FROM derived_upserts_data
  UNION ALL
  SELECT
    nct_id,
    NULL as derived_section_norm_json,
    'DELETE' as operation
  FROM derived_deletions_data
) AS source
ON target.nct_id = source.nct_id
WHEN MATCHED AND source.operation = 'DELETE' THEN DELETE
WHEN MATCHED AND source.operation = 'UPSERT' THEN UPDATE SET
  derived_section_norm_json = source.derived_section_norm_json
WHEN NOT MATCHED AND source.operation = 'UPSERT' THEN INSERT (
  nct_id,
  derived_section_norm_json
) VALUES (
  source.nct_id,
  source.derived_section_norm_json
);

-- @@

-- Show summary of changes
SELECT
    'DERIVED TABLE CHANGES' as table_name,
    operation_type,
    COUNT(*) as record_count
FROM derived_upserts_data
GROUP BY operation_type

UNION ALL

SELECT
    'DERIVED TABLE CHANGES' as table_name,
    'DELETIONS' as operation_type,
    COUNT(*) as record_count
FROM derived_deletions_data;
