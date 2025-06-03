-- Count upserts and deletions from diff parquet files
-- Returns summary of changes in standardized format

WITH change_counts AS (
    SELECT
        'CORE UPSERTS' as type,
        COUNT(*) as count
    FROM read_parquet('brick/diff/upserts.parquet')
    WHERE list_has_any(columns_to_update, ['core_study_record'])

    UNION ALL

    SELECT
        'DERIVED UPSERTS' as type,
        COUNT(*) as count
    FROM read_parquet('brick/diff/upserts.parquet')
    WHERE list_has_any(columns_to_update, ['derived_section_norm_json'])

    UNION ALL

    SELECT
        'DELETIONS' as type,
        COUNT(*) as count
    FROM read_parquet('brick/diff/deletions.parquet')
)
SELECT * FROM change_counts
UNION ALL
SELECT
    'TOTAL UPSERTS' as type,
    COUNT(*) as count
FROM read_parquet('brick/diff/upserts.parquet');
