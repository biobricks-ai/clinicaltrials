#!/usr/bin/env bash

set -eu -o pipefail

# Script to apply diff changes to Iceberg tables using Spark

[ -f ./.env ] && . ./.env

# Get local path
localpath=$(pwd)
echo "Local path: $localpath"

# Check if diff files exist
export diffpath="$localpath/brick/diff"
if [ ! -f "$diffpath/upserts.parquet" ] || [ ! -f "$diffpath/deletions.parquet" ]; then
    echo "Error: Diff files not found. Run stages/03_diff.sh first."
    exit 1
fi

# Check if version metadata exists
if [ ! -f "$diffpath/version_metadata.parquet" ]; then
    echo "Error: Version metadata not found. Run stages/03_diff.sh first."
    exit 1
fi

# Extract version information from metadata (just for display)
version_holder=$(duckdb -ascii -noheader -c "$(envsubst '${diffpath}' <<'EOF'
SELECT
json_extract_string(version_holder_json, '$.derivedSection.miscInfoModule.versionHolder')
FROM read_parquet('${diffpath}/version_metadata.parquet')
LIMIT 1
;
EOF
)"
)

echo "Version holder: $version_holder"

# Generic SQL to build commit properties using environment variables
COMMIT_PROP_DUCKDB_SQL="$(envsubst '${diffpath}' <<'EOF'
    WITH version_data AS (
        SELECT
            json_extract_string(version_holder_json, '$.derivedSection.miscInfoModule.versionHolder') as version_holder,
            version_holder_json
        FROM read_parquet('${diffpath}/version_metadata.parquet')
        LIMIT 1
    )
    SELECT json_object(
        'ctgov.version-holder', version_holder,
        'ctgov.version-holder-json', json_quote(version_holder_json),
        'ctgov.tables-updated', json_array(getenv('CTGOV_TABLE_NAME')) :: VARCHAR
    )
    FROM version_data
EOF
)"

# Check if Iceberg tables exist, create if not
core_table_path="$localpath/brick/iceberg/ctg_studies_core"
derived_table_path="$localpath/brick/iceberg/ctg_studies_derived"

if [ ! -d "$core_table_path" ] || [ ! -d "$derived_table_path" ]; then
    echo "Creating Iceberg tables..."
    stages/spark/bin/run_sql.sh 'stages/spark/sql/create_ctg_studies.sql'
    echo "Iceberg tables created."
fi

# Check which tables have changes
core_changes=$(duckdb -ascii -noheader -c "SELECT COUNT(*) FROM read_parquet('$diffpath/upserts.parquet') WHERE list_has_any(columns_to_update, ['core_study_record'])")
derived_changes=$(duckdb -ascii -noheader -c "SELECT COUNT(*) FROM read_parquet('$diffpath/upserts.parquet') WHERE list_has_any(columns_to_update, ['derived_section_norm_json'])")
deletions=$(duckdb -ascii -noheader -c "SELECT COUNT(*) FROM read_parquet('$diffpath/deletions.parquet')")

echo "Core changes: $core_changes"
echo "Derived changes: $derived_changes"
echo "Deletions: $deletions"

# Apply core table changes if any
if [ "$core_changes" -gt 0 ] || [ "$deletions" -gt 0 ]; then
    echo "=== APPLYING CORE TABLE CHANGES ==="

    # Build commit properties for core table
    export CTGOV_TABLE_NAME="ctg_studies_core"
    commit_props=$(duckdb -ascii -noheader -c "$COMMIT_PROP_DUCKDB_SQL")

    stages/spark/bin/run_sql.sh 'stages/spark/sql/apply_core_changes.sql' "$commit_props"
else
    echo "No core table changes to apply."
fi

# Apply derived table changes if any
if [ "$derived_changes" -gt 0 ] || [ "$deletions" -gt 0 ]; then
    echo "=== APPLYING DERIVED TABLE CHANGES ==="

    # Build commit properties for derived table
    export CTGOV_TABLE_NAME="ctg_studies_derived"
    commit_props=$(duckdb -ascii -noheader -c "$COMMIT_PROP_DUCKDB_SQL")

    stages/spark/bin/run_sql.sh 'stages/spark/sql/apply_derived_changes.sql' "$commit_props"
else
    echo "No derived table changes to apply."
fi

# Get final table statistics
echo "=== FINAL TABLE STATISTICS ==="
stages/spark/bin/run_sql.sh 'stages/spark/sql/table_stats.sql'

echo "Iceberg table update complete."
