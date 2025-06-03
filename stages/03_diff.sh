#!/usr/bin/env bash

set -eu -o pipefail

# Script to generate diff between ctg-studies.parquet and Iceberg table

[ -f ./.env ] && . ./.env

export DEFAULT_DUCKDB_MEMORY_LIMIT=$(duckdb -ascii -noheader  -c "SELECT current_setting('memory_limit');");

export CTGOV_DUCKDB_TEMP_DIR=${CTGOV_DUCKDB_TEMP_DIR:-}
export CTGOV_DUCKDB_MEMORY_LIMIT=${CTGOV_DUCKDB_MEMORY_LIMIT:-${DEFAULT_DUCKDB_MEMORY_LIMIT}}

# Get local path
localpath=$(pwd)
echo "Local path: $localpath"

# Validate version consistency before proceeding
echo "=== VERSION CONSISTENCY CHECK ==="
version_check_output=$(duckdb -ascii -noheader -f stages/sql/validate_version_consistency.sql)
echo "$version_check_output"

# Check if validation passed (look for "ok" in TAP output)
if ! echo "$version_check_output" | grep -q "^ok "; then
    echo "ERROR: Version consistency check failed. Aborting diff generation."
    exit 1
fi

# Create diff output directory
export diffpath="$localpath/brick/diff"
mkdir -p "$diffpath"
echo "Diff path: $diffpath"

# Check if Iceberg table exists, create empty one if not
iceberg_path="$localpath/brick/iceberg/ctg_studies"
if [ ! -d "$iceberg_path" ]; then
    echo "Iceberg table does not exist. Creating empty table first..."

    # Create empty Iceberg table using shared script
    stages/spark/bin/run_sql.sh 'stages/spark/sql/create_ctg_studies.sql'

    echo "Empty Iceberg table created at $iceberg_path"
fi

echo "Running diff against Iceberg table"

# Run the diff SQL script
duckdb -c ".read stages/sql/diff_ctg_studies.sql"

echo "Generated diff files in $diffpath"

# Display summary
echo "=== DIFF SUMMARY ==="
duckdb -batch < stages/sql/count_changes.sql

echo ""
echo "=== COLUMN CHANGE ANALYSIS ==="
duckdb -batch < stages/sql/analyze_column_changes.sql

echo "Diff generation complete."
