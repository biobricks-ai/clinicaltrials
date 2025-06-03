-- Analyze which columns are changing in upserts
-- Returns breakdown of column change patterns

SELECT
    CASE
        WHEN list_has_any(columns_to_update, ['core_study_record'])
         AND list_has_any(columns_to_update, ['derived_section_norm_json'])
        THEN 'BOTH'
        WHEN list_has_any(columns_to_update, ['core_study_record'])
        THEN 'CORE_ONLY'
        WHEN list_has_any(columns_to_update, ['derived_section_norm_json'])
        THEN 'DERIVED_ONLY'
        ELSE 'UNKNOWN'
    END as change_pattern,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM read_parquet('brick/diff/upserts.parquet')
GROUP BY change_pattern
ORDER BY record_count DESC;
