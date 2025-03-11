#!/usr/bin/env bash

# Script to download files

# Get local path
localpath=$(pwd)
echo "Local path: $localpath"


# Create the download directory
export downloadpath="$localpath/download"
echo "Download path: $downloadpath"
mkdir -p "$downloadpath"

# Download zipped 
wget -P $downloadpath --content-disposition https://clinicaltrials.gov/api/v2/studies/download?format=json.zip

echo "Download done."
