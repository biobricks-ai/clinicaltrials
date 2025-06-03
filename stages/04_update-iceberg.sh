#!/usr/bin/env bash

set -eu -o pipefail

# Script to apply diff changes to Iceberg table using Spark

# Get local path
localpath=$(pwd)
echo "Local path: $localpath"

# Check if diff files exist
diffpath="$localpath/brick/diff"
if [ ! -f "$diffpath/upserts.parquet" ] || [ ! -f "$diffpath/deletions.parquet" ]; then
    echo "Error: Diff files not found. Run stages/03_diff.sh first."
    exit 1
fi

# Use shared Spark SQL runner
spark_run_sql() {
    stages/spark/bin/run_sql.sh "$1"
}

# Check if Iceberg table exists, create if not
iceberg_path="$localpath/brick/iceberg/ctg_studies"
if [ ! -d "$iceberg_path" ]; then
    echo "Creating Iceberg table..."
    spark_run_sql 'stages/spark/sql/create_ctg_studies.sql'
    echo "Iceberg table created."
fi

# Apply all changes in a single transaction
echo "Applying all changes (deletions and upserts) in transaction..."
spark_run_sql 'stages/spark/sql/apply_all_changes.sql'

# Get final table statistics
echo "=== FINAL TABLE STATISTICS ==="
spark_run_sql 'stages/spark/sql/table_stats.sql'

echo "Iceberg table update complete."
