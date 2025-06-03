-- Validate that all records in the dataset have consistent versionHolder values
-- This ensures data integrity before processing updates
-- Output in TAP (Test Anything Protocol) format
--
-- ```
--   prove -v -e 'duckdb -ascii -noheader -f' stages/sql/validate_version_consistency.sql
-- ```

WITH version_holders AS (
    SELECT DISTINCT
        json_extract_string(study_record, '$.derivedSection.miscInfoModule.versionHolder') as version_holder
    FROM read_parquet('brick/ctg-studies.parquet/*.parquet')
    WHERE json_extract_string(study_record, '$.derivedSection.miscInfoModule.versionHolder') IS NOT NULL
),
version_count AS (
    SELECT
        COUNT(*) as distinct_versions,
        MIN(version_holder) as version_holder
    FROM version_holders
)
SELECT 'TAP version 13' as output
UNION ALL
SELECT '1..1' as output
UNION ALL
SELECT
    CASE
        WHEN distinct_versions = 0 THEN 'not ok 1 - No versionHolder found in dataset'
        WHEN distinct_versions = 1 THEN 'ok 1 - Single versionHolder found: ' || version_holder
        ELSE 'not ok 1 - Multiple versionHolders found (' || distinct_versions || ' distinct values)'
    END as output
FROM version_count;
