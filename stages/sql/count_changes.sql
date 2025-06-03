-- Count upserts and deletions from diff parquet files
-- Returns summary of changes in standardized format

SELECT 'UPSERTS' as type, COUNT(*) as count
FROM read_parquet('brick/diff/upserts.parquet')
UNION ALL
SELECT 'DELETIONS' as type, COUNT(*) as count
FROM read_parquet('brick/diff/deletions.parquet');
