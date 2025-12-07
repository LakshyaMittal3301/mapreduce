#!/usr/bin/env bash

set -euo pipefail

if [ $# -ne 2 ]; then
    echo "Usage: $0 <app-name-or-go-file> <s3-bucket>"
    echo "Example: $0 wc my-mapreduce-bucket"
    exit 1
fi

APP_ARG="$1"        # e.g. wc or wc.go
BUCKET="$2"
LOG_LEVEL="${LOG_LEVEL:-info}"

APP_BASE="${APP_ARG%.go}"   # wc.go -> wc, wc -> wc
APP_GO="${APP_BASE}.go"     # wc -> wc.go
APP_SO="${APP_BASE}.so"     # wc -> wc.so

# Root of the repo: .../map_reduce
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BIN_DIR="${ROOT_DIR}/bin"
PLUGINS_DIR="${BIN_DIR}/plugins"
TMP_ROOT="${ROOT_DIR}/tmp"
WORKDIR="${TMP_ROOT}/mr-s3-single"

mkdir -p "$BIN_DIR" "$PLUGINS_DIR" "$WORKDIR"
rm -rf "$WORKDIR"/*
cd "$WORKDIR"

echo "*** Building plugin for ${APP_GO}"
(
  cd "${ROOT_DIR}/apps"
  go build -buildmode=plugin -o "${PLUGINS_DIR}/${APP_SO}" "${APP_GO}"
)

echo "*** Building coordinator/worker/sequential"
(
  cd "${ROOT_DIR}/cmd"
  go build -o "${BIN_DIR}/mrcoordinator" mrcoordinator.go
  go build -o "${BIN_DIR}/mrworker"      mrworker.go
  go build -o "${BIN_DIR}/mrsequential"  mrsequential.go
)

echo "*** Generating expected output with local mrsequential"
"${BIN_DIR}/mrsequential" "${PLUGINS_DIR}/${APP_SO}" "${ROOT_DIR}/data/pg/pg-"*.txt

if [ -f mr-out-0 ]; then
    sort mr-out-0 > mr-expected
    rm -f mr-out-*
else
    echo "ERROR: mrsequential did not produce mr-out-0"
    exit 1
fi

echo "*** Running distributed MapReduce with S3 backend"
COORD_ADDR="localhost:8123"

# Start coordinator (uses local files as input as usual)
"${BIN_DIR}/mrcoordinator" \
  -n-reduce=10 \
  -job-id="s3test" \
  -listen=":${COORD_ADDR##*:}" \
  -log-level="${LOG_LEVEL}" \
  "${ROOT_DIR}/data/pg/pg-"*.txt &
CID=$!

# Give coordinator time to start
sleep 1

# Start a few S3 workers
for i in 1 2 3; do
  "${BIN_DIR}/mrworker" \
    -coord-addr="${COORD_ADDR}" \
    -storage="s3" \
    -s3-bucket="${BUCKET}" \
    -log-level="${LOG_LEVEL}" \
    -app="${PLUGINS_DIR}/${APP_SO}" &
done

# Wait for coordinator to finish
wait "${CID}" || true

# Wait for workers to exit
wait || true

echo "*** Job finished. Checking S3 output (if aws cli is configured)..."
echo "*** Listing: s3://${BUCKET}/jobs/"
aws s3 ls "s3://${BUCKET}/jobs/" || true

echo
echo "Now check: s3://${BUCKET}/jobs/<job-id>/output/mr-out-*"
echo "If you want, you can download and compare with mr-expected."
