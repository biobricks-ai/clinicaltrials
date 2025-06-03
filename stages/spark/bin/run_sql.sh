#!/usr/bin/env bash

# Shared utility script to run Spark SQL files

set -eu -o pipefail

# Source environment variables
[ -f .env ] && source .env

# Set default temp directory if not specified
export CTGOV_SPARK_TEMP_DIR="${CTGOV_SPARK_TEMP_DIR:-/tmp/spark-ctgov}"

# Check arguments
if [ $# -lt 1 ] || [ $# -gt 2 ]; then
    echo "Usage: run_sql.sh <sql_file> [commit_properties_json]"
    echo "Example: run_sql.sh apply_core_changes.sql '{\"version-holder\": \"2025-05-30\"}'"
    exit 1
fi
[ -f "$1" ] || { echo "Error: $1 is not a file"; exit 1; }

# Export commit properties if provided
if [ $# -eq 2 ]; then
    export COMMIT_PROPERTIES="$2"
    echo "Using commit properties: $COMMIT_PROPERTIES"
fi

# Create temporary config file with expanded variables using envsubst
temp_conf=$(mktemp)
trap "rm -f $temp_conf" EXIT
envsubst '$CTGOV_SPARK_TEMP_DIR' < stages/spark/iceberg.conf > "$temp_conf"

# Set verbose flag if requested
verbose_flag=""
[ "${SPARK_RUN_SQL_VERBOSE:-0}" -eq 1 ] 2>/dev/null && verbose_flag="-v"

# Run Spark SQL
SQL_FILE="$1" \
spark-shell \
    --properties-file "$temp_conf" \
    -i stages/spark/bin/run_sql.scala \
    "$verbose_flag"
