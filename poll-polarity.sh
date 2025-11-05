#!/bin/bash

set -e
set -o pipefail

while true; do
  RFC3339=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  PARQUET_FILE="polarity-$RFC3339.parquet"

  echo "Starting polarity data update at $RFC3339"
	uv run update-polarity.py --output "${PARQUET_FILE}"
	uv run load-database.py --input "${PARQUET_FILE}"
done
