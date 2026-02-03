#!/usr/bin/env bash

set -euo pipefail

# Usage:
#   ./scripts/run-coordinator-ec2.sh <n-reduce> <job-id-prefix> <s3-bucket> <input-prefix>
#
# Example:
#   ./scripts/run-coordinator-ec2.sh 10 wc-demo rc-mapreduce-bucket inputs/pg
#
# It will:
#   - List objects under s3://<bucket>/<input-prefix>/
#   - Extract the filenames and pass them to mrcoordinator
#   - Listen on :8123 by default (overridable via LISTEN_PORT env var)
#   - Use configurable defaults for log-level/map/reduce timeouts

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <n-reduce> <job-id-prefix> <s3-bucket> <input-prefix>"
  echo "Example: $0 10 wc-demo rc-mapreduce-bucket inputs/pg"
  exit 1
fi

NREDUCE="$1"
JOB_PREFIX="$2"
BUCKET="$3"
INPUT_PREFIX="$4"

# ---- Defaults (can be overridden via env or by editing this script) ----
LOG_LEVEL="${LOG_LEVEL:-info}"
MAP_TIMEOUT="${MAP_TIMEOUT:-10s}"
REDUCE_TIMEOUT="${REDUCE_TIMEOUT:-10s}"
LISTEN_PORT="${LISTEN_PORT:-8123}"
LISTEN_ADDR=":${LISTEN_PORT}"
# ------------------------------------------------------------------------

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${ROOT_DIR}/bin"
mkdir -p "${BIN_DIR}"

echo "*** Discovering input files from S3: s3://${BUCKET}/${INPUT_PREFIX}/"
INPUT_FILES=()
while read -r line; do
  name=$(echo "$line" | awk '{print $4}')
  if [ -n "$name" ]; then
    INPUT_FILES+=("$name")
  fi
done < <(aws s3 ls "s3://${BUCKET}/${INPUT_PREFIX}/")

if [ "${#INPUT_FILES[@]}" -eq 0 ]; then
  echo "ERROR: no input files found in s3://${BUCKET}/${INPUT_PREFIX}/"
  exit 1
fi

echo "*** Found ${#INPUT_FILES[@]} input files: ${INPUT_FILES[*]}"

# ---- Check if port is already in use ----
if command -v lsof >/dev/null 2>&1; then
  if lsof -iTCP:"${LISTEN_PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "ERROR: port ${LISTEN_PORT} is already in use. Refusing to start mrcoordinator."
    echo "Hint: you can run something like:"
    echo "  pkill mrcoordinator"
    echo "or inspect with:"
    echo "  lsof -iTCP:${LISTEN_PORT} -sTCP:LISTEN"
    exit 1
  fi
else
  echo "WARN: lsof not installed; skipping port-in-use check."
fi
# ----------------------------------------

echo "*** Building mrcoordinator binary"
(
  cd "${ROOT_DIR}/cmd"
  go build -o "${BIN_DIR}/mrcoordinator" mrcoordinator.go
)

echo "*** Starting coordinator"
echo "  nReduce       : ${NREDUCE}"
echo "  job id prefix : ${JOB_PREFIX}"
echo "  listen addr   : ${LISTEN_ADDR}"
echo "  log level     : ${LOG_LEVEL}"
echo "  map timeout   : ${MAP_TIMEOUT}"
echo "  reduce timeout: ${REDUCE_TIMEOUT}"
echo "  S3 bucket     : ${BUCKET}"
echo "  S3 input pref.: ${INPUT_PREFIX}"
echo "  input files   : ${INPUT_FILES[*]}"

"${BIN_DIR}/mrcoordinator" \
  -n-reduce="${NREDUCE}" \
  -job-id="${JOB_PREFIX}" \
  -listen="${LISTEN_ADDR}" \
  -log-level="${LOG_LEVEL}" \
  -map-timeout="${MAP_TIMEOUT}" \
  -reduce-timeout="${REDUCE_TIMEOUT}" \
  "${INPUT_FILES[@]}"