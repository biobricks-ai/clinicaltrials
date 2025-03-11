#!/usr/bin/env bash

# Script to process unzipped files and build parquet files

# Get local path
localpath=$(pwd)
echo "Local path: $localpath"

# Set download path
export downloadpath="$localpath/download"
echo "Download path: $downloadpath"

# Set raw path
export rawpath="$localpath/raw"
echo "Raw path: $rawpath"

# Create brick directory
export brickpath="$localpath/brick"
mkdir -p $brickpath
echo "Brick path: $brickpath"

[ -d "$rawpath" ] && rm -R $rawpath

# Unzip 
unzip -q -d $rawpath $downloadpath/ctg-studies.json.zip


# Create 4-digit NCT buckets
find raw -type f -printf "%f\n" \
  | grep -P -o 'NCT\d{4}' \
  | sort -u \
  | parallel --bar '
      export PREFIX={};
      #echo "Working on prefix: $PREFIX";
      < stages/NCT_json2parquet_prefixed.sql \
	      perl -pe "
          s,NCT_PREFIX_JSON,\$ENV{PREFIX}, ;
          s,NCT_PREFIX_PARQUET,\$ENV{PREFIX}_, ;
        " \
        | duckdb
    '
