-- Diff script to compare ctg-studies.parquet with existing Iceberg table
-- and generate upserts and deletions using DuckDB

SET temp_directory = getenv('CTGOV_DUCKDB_TEMP_DIR');
SET memory_limit = getenv('CTGOV_DUCKDB_MEMORY_LIMIT');

SET preserve_insertion_order = false;

-- -- Enable profiling to see query performance
-- SET enable_profiling = 'query_tree';
-- SET profiling_mode = 'detailed';

-- Load extensions
INSTALL iceberg;
LOAD iceberg;

INSTALL avro;
LOAD avro;

INSTALL json;
LOAD json;

-- Macro to extract arrays that need sorting from any data source
CREATE OR REPLACE MACRO extract_array_for_sorting(data_source, array_path) AS TABLE (
    SELECT
        nct_id,
        array_path as array_path,
        json_extract(study_record, '$.derivedSection.' || array_path) as array_data
    FROM query_table(data_source)
    WHERE json_array_length(json_extract(study_record, '$.derivedSection.' || array_path)) > 1
);

-- Macro to extract all arrays from a data source using CTE
CREATE OR REPLACE MACRO extract_all_arrays_for_sorting(data_source) AS TABLE (
    WITH raw_data AS (
        SELECT nct_id, study_record FROM query_table(data_source)
    )
    SELECT * FROM extract_array_for_sorting(raw_data, 'conditionBrowseModule.meshes')
    UNION ALL
    SELECT * FROM extract_array_for_sorting(raw_data, 'conditionBrowseModule.ancestors')
    UNION ALL
    SELECT * FROM extract_array_for_sorting(raw_data, 'conditionBrowseModule.browseLeaves')
    UNION ALL
    SELECT * FROM extract_array_for_sorting(raw_data, 'conditionBrowseModule.browseBranches')
    UNION ALL
    SELECT * FROM extract_array_for_sorting(raw_data, 'interventionBrowseModule.meshes')
    UNION ALL
    SELECT * FROM extract_array_for_sorting(raw_data, 'interventionBrowseModule.ancestors')
    UNION ALL
    SELECT * FROM extract_array_for_sorting(raw_data, 'interventionBrowseModule.browseLeaves')
    UNION ALL
    SELECT * FROM extract_array_for_sorting(raw_data, 'interventionBrowseModule.browseBranches')
);

-- Export arrays that need sorting from source data only
-- Note: Target data (Iceberg) is already normalized, so no sorting needed
CREATE OR REPLACE VIEW source_arrays_to_sort AS
WITH source_data AS (
    SELECT nct_id, study_record FROM read_parquet('brick/ctg-studies.parquet/*.parquet')
)
SELECT * FROM extract_all_arrays_for_sorting(source_data);

-- Combined view of all arrays that need sorting (only source data)
CREATE OR REPLACE VIEW all_arrays_to_sort AS
SELECT 'source' as data_source, * FROM source_arrays_to_sort;

-- Export arrays to sort
SELECT 'Exporting arrays to sort...' as status;
COPY (SELECT * FROM all_arrays_to_sort) TO 'brick/diff/arrays_to_sort.parquet' (FORMAT PARQUET);

-- Create sorted arrays
CREATE OR REPLACE VIEW sorted_arrays AS
SELECT
    data_source,
    nct_id,
    array_path,
    CASE
        WHEN array_path LIKE '%.browseBranches' THEN
            '[' || string_agg(array_element::VARCHAR, ',' ORDER BY json_extract(array_element, '$.abbrev')) || ']'
        ELSE
            '[' || string_agg(array_element::VARCHAR, ',' ORDER BY json_extract(array_element, '$.id')) || ']'
    END as sorted_array_json
FROM (
    SELECT
        data_source,
        nct_id,
        array_path,
        unnest(json_extract(array_data, '$[*]')) as array_element
    FROM read_parquet('brick/diff/arrays_to_sort.parquet')
)
GROUP BY data_source, nct_id, array_path;

-- Export sorted arrays
SELECT 'Exporting sorted arrays...' as status;
COPY (SELECT * FROM sorted_arrays) TO 'brick/diff/sorted_arrays.parquet' (FORMAT PARQUET);

-- Fast macro to reconstruct normalized derivedSection from pre-sorted arrays
CREATE OR REPLACE MACRO normalize_derived_section_from_sorted(nct_id_param, derived_section) AS (
    WITH sorted_lookup AS (
        SELECT nct_id, array_path, sorted_array_json
        FROM read_parquet('brick/diff/sorted_arrays.parquet')
        WHERE nct_id = nct_id_param
    ),
    base AS (
        SELECT
            derived_section,
            json_extract(derived_section, '$.conditionBrowseModule')    AS cond_module,
            json_extract(derived_section, '$.interventionBrowseModule') AS int_module
    )
    SELECT
        CASE
            WHEN derived_section IS NULL THEN NULL
            ELSE json_object(
                'derivedSection',
                json_merge_patch(
                    json_merge_patch(derived_section, '{"miscInfoModule": {"versionHolder": null}}'),
                    json_object(
                        'conditionBrowseModule',
                        CASE
                            WHEN cond_module IS NULL THEN NULL
                            ELSE json_merge_patch(
                                cond_module,
                                json_object(
                                    'meshes',         COALESCE((SELECT sorted_array_json FROM sorted_lookup WHERE array_path = 'conditionBrowseModule.meshes'), json_extract(cond_module, '$.meshes')),
                                    'ancestors',      COALESCE((SELECT sorted_array_json FROM sorted_lookup WHERE array_path = 'conditionBrowseModule.ancestors'), json_extract(cond_module, '$.ancestors')),
                                    'browseLeaves',   COALESCE((SELECT sorted_array_json FROM sorted_lookup WHERE array_path = 'conditionBrowseModule.browseLeaves'), json_extract(cond_module, '$.browseLeaves')),
                                    'browseBranches', COALESCE((SELECT sorted_array_json FROM sorted_lookup WHERE array_path = 'conditionBrowseModule.browseBranches'), json_extract(cond_module, '$.browseBranches'))
                                )
                            )
                        END,
                        'interventionBrowseModule',
                        CASE
                            WHEN int_module IS NULL THEN NULL
                            ELSE json_merge_patch(
                                int_module,
                                json_object(
                                    'meshes',         COALESCE((SELECT sorted_array_json FROM sorted_lookup WHERE array_path = 'interventionBrowseModule.meshes'), json_extract(int_module, '$.meshes')),
                                    'ancestors',      COALESCE((SELECT sorted_array_json FROM sorted_lookup WHERE array_path = 'interventionBrowseModule.ancestors'), json_extract(int_module, '$.ancestors')),
                                    'browseLeaves',   COALESCE((SELECT sorted_array_json FROM sorted_lookup WHERE array_path = 'interventionBrowseModule.browseLeaves'), json_extract(int_module, '$.browseLeaves')),
                                    'browseBranches', COALESCE((SELECT sorted_array_json FROM sorted_lookup WHERE array_path = 'interventionBrowseModule.browseBranches'), json_extract(int_module, '$.browseBranches'))
                                )
                            )
                        END
                    )
                )
            )
        END::VARCHAR
    FROM base
);

-- Macro to extract and hash study record components for diffing using pre-sorted arrays
CREATE OR REPLACE MACRO extract_study_components(table_source, data_source_name) AS TABLE (
    WITH sorted_lookup AS (
        SELECT nct_id, array_path, sorted_array_json
        FROM read_parquet('brick/diff/sorted_arrays.parquet')
        WHERE data_source = data_source_name
    ),
    study_data AS (
        SELECT nct_id, study_record FROM query_table(table_source)
    ),
    normalized_derived AS (
        SELECT
            s.nct_id,
            s.study_record,
            json_object('derivedSection',
                json_merge_patch(
                    json_merge_patch(json_extract(s.study_record, '$.derivedSection'), '{"miscInfoModule": {"versionHolder": null}}'),
                    json_object(
                        'conditionBrowseModule',
                        CASE WHEN json_extract(s.study_record, '$.derivedSection.conditionBrowseModule') IS NULL THEN NULL
                        ELSE json_merge_patch(
                            json_extract(s.study_record, '$.derivedSection.conditionBrowseModule'),
                            json_object(
                                'meshes', COALESCE(max(CASE WHEN sl.array_path = 'conditionBrowseModule.meshes' THEN sl.sorted_array_json END), json_extract(s.study_record, '$.derivedSection.conditionBrowseModule.meshes')),
                                'ancestors', COALESCE(max(CASE WHEN sl.array_path = 'conditionBrowseModule.ancestors' THEN sl.sorted_array_json END), json_extract(s.study_record, '$.derivedSection.conditionBrowseModule.ancestors')),
                                'browseLeaves', COALESCE(max(CASE WHEN sl.array_path = 'conditionBrowseModule.browseLeaves' THEN sl.sorted_array_json END), json_extract(s.study_record, '$.derivedSection.conditionBrowseModule.browseLeaves')),
                                'browseBranches', COALESCE(max(CASE WHEN sl.array_path = 'conditionBrowseModule.browseBranches' THEN sl.sorted_array_json END), json_extract(s.study_record, '$.derivedSection.conditionBrowseModule.browseBranches'))
                            )
                        ) END,
                        'interventionBrowseModule',
                        CASE WHEN json_extract(s.study_record, '$.derivedSection.interventionBrowseModule') IS NULL THEN NULL
                        ELSE json_merge_patch(
                            json_extract(s.study_record, '$.derivedSection.interventionBrowseModule'),
                            json_object(
                                'meshes', COALESCE(max(CASE WHEN sl.array_path = 'interventionBrowseModule.meshes' THEN sl.sorted_array_json END), json_extract(s.study_record, '$.derivedSection.interventionBrowseModule.meshes')),
                                'ancestors', COALESCE(max(CASE WHEN sl.array_path = 'interventionBrowseModule.ancestors' THEN sl.sorted_array_json END), json_extract(s.study_record, '$.derivedSection.interventionBrowseModule.ancestors')),
                                'browseLeaves', COALESCE(max(CASE WHEN sl.array_path = 'interventionBrowseModule.browseLeaves' THEN sl.sorted_array_json END), json_extract(s.study_record, '$.derivedSection.interventionBrowseModule.browseLeaves')),
                                'browseBranches', COALESCE(max(CASE WHEN sl.array_path = 'interventionBrowseModule.browseBranches' THEN sl.sorted_array_json END), json_extract(s.study_record, '$.derivedSection.interventionBrowseModule.browseBranches'))
                            )
                        ) END
                    )
                )
            )::VARCHAR as derived_section_norm_json
        FROM study_data s
        LEFT JOIN sorted_lookup sl ON s.nct_id = sl.nct_id
        GROUP BY s.nct_id, s.study_record
    ),
    version_json_builder AS (
        SELECT
            *,
            json_object('derivedSection',
                json_object('miscInfoModule',
                    json_object('versionHolder',
                        json_extract(study_record, '$.derivedSection.miscInfoModule.versionHolder'))))::VARCHAR AS version_holder_json
        FROM normalized_derived
    )
    SELECT
        nct_id,
        -- Core study record without derivedSection
        json_merge_patch(study_record, '{"derivedSection": null}') AS core_study_record,
        -- Normalized derivedSection (excluding versionHolder) using pre-sorted arrays
        derived_section_norm_json,
        -- Version holder as separate JSON object that can be merged back
        version_holder_json,
        -- Hash each component separately for comparison
        md5(json_merge_patch(study_record, '{"derivedSection": null}')::VARCHAR) AS core_hash,
        md5(derived_section_norm_json) AS derived_hash,
        md5(version_holder_json) AS version_hash
    FROM version_json_builder
);

-- Create views for source and target data using CTEs with the macro
CREATE OR REPLACE VIEW source_data AS
WITH raw_data AS (
    SELECT nct_id, study_record
    FROM read_parquet('brick/ctg-studies.parquet/*.parquet')
    -- ORDER BY nct_id LIMIT 10
)
SELECT * FROM extract_study_components(raw_data, 'source');

-- Prepare source data with split columns for two-table architecture
CREATE OR REPLACE VIEW source_data_split AS
WITH raw_data AS (
    SELECT nct_id, study_record
    FROM read_parquet('brick/ctg-studies.parquet/*.parquet')
)
SELECT
    nct_id,
    core_study_record,
    derived_section_norm_json,
    version_holder_json,
    core_hash,
    derived_hash,
    version_hash
FROM extract_study_components(raw_data, 'source');

-- Create views for target data from both tables
CREATE OR REPLACE VIEW target_core_data AS
SELECT
    nct_id,
    core_study_record,
    md5(core_study_record::VARCHAR) AS core_hash
FROM iceberg_scan('brick/iceberg/ctg_studies_core', version = 'version-hint.text');


CREATE OR REPLACE VIEW target_derived_data AS
SELECT
    nct_id,
    derived_section_norm_json,
    md5(derived_section_norm_json::VARCHAR) AS derived_hash
FROM iceberg_scan('brick/iceberg/ctg_studies_derived', version = 'version-hint.text');

-- Combine target data with full hash comparison
CREATE OR REPLACE VIEW target_data_combined AS
SELECT
    COALESCE(c.nct_id, d.nct_id) as nct_id,
    c.core_study_record,
    d.derived_section_norm_json,
    c.core_hash,
    d.derived_hash
FROM target_core_data c
FULL OUTER JOIN target_derived_data d ON c.nct_id = d.nct_id;

-- Generate unified upserts with columns_to_update pattern
CREATE OR REPLACE VIEW upserts AS
SELECT
    s.nct_id,
    s.core_study_record,
    s.derived_section_norm_json,
    s.version_holder_json,
    CASE
        WHEN t.nct_id IS NULL THEN 'INSERT'
        WHEN s.core_hash != COALESCE(t.core_hash, '')
            OR s.derived_hash != COALESCE(t.derived_hash, '')
            THEN 'UPDATE'
    END as operation_type,
    -- Include change details for debugging
    CASE WHEN t.nct_id IS NULL OR s.core_hash != COALESCE(t.core_hash, '') THEN true ELSE false END as core_changed,
    CASE WHEN t.nct_id IS NULL OR s.derived_hash != COALESCE(t.derived_hash, '') THEN true ELSE false END as derived_changed,
    -- Generate list of columns to update (matching table names)
    CASE
        WHEN t.nct_id IS NULL THEN ['core_study_record', 'derived_section_norm_json']
        ELSE (
            SELECT list(col_name)
            FROM (
                SELECT
                    unnest( CASE WHEN s.core_hash != COALESCE(t.core_hash, '') THEN ['core_study_record'] ELSE [] END
                          + CASE WHEN s.derived_hash != COALESCE(t.derived_hash, '') THEN ['derived_section_norm_json'] ELSE [] END
                    ) as col_name
            )
        )
    END as columns_to_update
FROM source_data_split s
LEFT JOIN target_data_combined t ON s.nct_id = t.nct_id
WHERE t.nct_id IS NULL
   OR s.core_hash != COALESCE(t.core_hash, '')
   OR s.derived_hash != COALESCE(t.derived_hash, '');

-- Generate unified deletions
CREATE OR REPLACE VIEW deletions AS
SELECT
    t.nct_id,
    'DELETE' as operation_type,
    CASE
        WHEN c.nct_id IS NOT NULL AND d.nct_id IS NOT NULL THEN ['core_study_record', 'derived_section_norm_json']
        WHEN c.nct_id IS NOT NULL THEN ['core_study_record']
        WHEN d.nct_id IS NOT NULL THEN ['derived_section_norm_json']
    END as columns_to_update
FROM target_data_combined t
LEFT JOIN source_data_split s ON t.nct_id = s.nct_id
LEFT JOIN target_core_data c ON t.nct_id = c.nct_id
LEFT JOIN target_derived_data d ON t.nct_id = d.nct_id
WHERE s.nct_id IS NULL;

-- Export unified upserts to parquet
SELECT 'Exporting upserts...' as status;
COPY (SELECT * FROM upserts) TO 'brick/diff/upserts.parquet' (FORMAT PARQUET);

-- Export unified deletions to parquet
SELECT 'Exporting deletions...' as status;
COPY (SELECT * FROM deletions) TO 'brick/diff/deletions.parquet' (FORMAT PARQUET);

-- Export version metadata separately
CREATE OR REPLACE VIEW version_metadata AS
SELECT DISTINCT version_holder_json
FROM source_data_split
WHERE version_holder_json IS NOT NULL;

SELECT 'Exporting version metadata...' as status;
COPY (SELECT * FROM version_metadata) TO 'brick/diff/version_metadata.parquet' (FORMAT PARQUET);
