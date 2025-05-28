-- syntax: DuckDB SQL
--
-- NAME
--
--   NCT_json2parquet_prefixed.sql - Load all JSONL data into a single Parquet file
--
-- DESCRIPTION
--
--   This processes all the versioned historical ClinicalTrials.gov study
--   record JSONL data files and put all valid records in a Parquet file to
--   speed up later processing steps.

INSTALL json;

LOAD json;

COPY (
    SELECT
            json->>'$.protocolSection.identificationModule.nctId' AS nct_id,
            json AS study_record
    FROM
        read_json_objects(
            'raw/NCT_PREFIX_JSON*.json',
            ignore_errors = false
        )
    ORDER BY nct_id
) TO 'brick/ctg-studies.parquet/NCT_PREFIX_PARQUET.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 100_000)
