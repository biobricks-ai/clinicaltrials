#!/usr/bin/env bash

# Shared utility script to run Spark SQL files

set -eu -o pipefail

# Source environment variables
[ -f .env ] && source .env

# Set default temp directory if not specified
export CTGOV_SPARK_TEMP_DIR="${CTGOV_SPARK_TEMP_DIR:-/tmp/spark-ctgov}"

# Check arguments
[ $# -eq 1 ] || { echo "Usage: run_sql.sh <sql_file>"; exit 1; }
[ -f "$1" ] || { echo "Error: $1 is not a file"; exit 1; }

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
