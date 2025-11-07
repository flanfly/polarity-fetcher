#!/bin/bash

set -e
set -o pipefail

while true; do
  RFC3339=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  PARQUET_FILE="polarity-$RFC3339.parquet"
  SAFE_PARQUET_FILE=$(echo "${PARQUET_FILE}" | tr ':' '_')

  echo "Starting polarity data update at $RFC3339"
  uv run update-polarity.py --output "${SAFE_PARQUET_FILE}" --log-level DEBUG --log-file "update-polarity-$RFC3339.log"

  echo "Loading polarity data into database from ${SAFE_PARQUET_FILE}"
  uv run load-database.py --input "${SAFE_PARQUET_FILE}"
done
