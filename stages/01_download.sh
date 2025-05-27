#!/usr/bin/env bash

set -eu -o pipefail

# Script to download files

# Get local path
localpath=$(pwd)
echo "Local path: $localpath"


# Create the download directory
export downloadpath="$localpath/download"
echo "Download path: $downloadpath"
mkdir -p "$downloadpath"

TARGET_FILE="$downloadpath"/ctg-studies.json.zip

# Download zipped 
# NOTE: Using `--content-disposition` then checking the file name here to make sure
# API's headers continue to work properly. This is instead of setting the
# filename directly.
wget -P $downloadpath --content-disposition https://clinicaltrials.gov/api/v2/studies/download?format=json.zip

[ -s "$TARGET_FILE" ] && echo "Downloaded to $TARGET_FILE"

echo "Download done."
