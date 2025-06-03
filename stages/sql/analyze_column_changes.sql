-- Analyze which columns are changing in upserts
-- Returns breakdown of column change patterns

SELECT
    columns_to_update,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM read_parquet('brick/diff/upserts.parquet')
GROUP BY columns_to_update
ORDER BY record_count DESC;
