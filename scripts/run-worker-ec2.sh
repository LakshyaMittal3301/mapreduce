#!/usr/bin/env bash

set -euo pipefail

# Usage:
#   ./scripts/run-workers-ec2.sh <app-name-or-go-file> <s3-bucket> <coord-addr> [num-workers]
#
# Example:
#   ./scripts/run-workers-ec2.sh wc mapreduce-bucket 10.0.1.23:8123 4
#
#   app-name-or-go-file : e.g. wc or wc.go
#   s3-bucket           : S3 bucket name (e.g. mapreduce-bucket)
#   coord-addr          : coordinator address, e.g. 10.0.1.23:8123 or my-elastic-ip:8123
#   num-workers         : optional, default 3

if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
  echo "Usage: $0 <app-name-or-go-file> <s3-bucket> <coord-addr> [num-workers]"
  echo "Example: $0 wc mapreduce-bucket 10.0.1.23:8123 4"
  exit 1
fi

APP_ARG="$1"
BUCKET="$2"
COORD_ADDR="$3"
NUM_WORKERS="${4:-3}"

# ---- Defaults (overridable via env or by editing this script) ----
LOG_LEVEL="${LOG_LEVEL:-info}"
S3_CONCURRENCY="${S3_CONCURRENCY:-16}"
S3_INPUT_PREFIX="${S3_INPUT_PREFIX:-inputs/pg}"
IDLE_WAIT="${IDLE_WAIT:-1s}"
# -----------------------------------------------------------------

APP_BASE="${APP_ARG%.go}"        # wc.go -> wc, wc -> wc
APP_GO="${APP_BASE}.go"          # wc -> wc.go
APP_SO="${APP_BASE}.so"          # wc -> wc.so

# Root of the repo: .../map_reduce
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BIN_DIR="${ROOT_DIR}/bin"
PLUGINS_DIR="${BIN_DIR}/plugins"

mkdir -p "${BIN_DIR}" "${PLUGINS_DIR}"

echo "*** Building mrworker binary"
(
  cd "${ROOT_DIR}/cmd"
  go build -o "${BIN_DIR}/mrworker" mrworker.go
)

echo "*** Building plugin for ${APP_GO}"
(
  cd "${ROOT_DIR}/apps"
  go build -buildmode=plugin -o "${PLUGINS_DIR}/${APP_SO}" "${APP_GO}"
)

echo "*** Starting ${NUM_WORKERS} worker(s)"
echo "  coord addr     : ${COORD_ADDR}"
echo "  bucket         : ${BUCKET}"
echo "  app            : ${APP_BASE}"
echo "  plugin         : ${PLUGINS_DIR}/${APP_SO}"
echo "  S3 input prefix: ${S3_INPUT_PREFIX}"
echo "  log level      : ${LOG_LEVEL}"
echo "  S3 concurrency : ${S3_CONCURRENCY}"
echo "  idle wait      : ${IDLE_WAIT}"

for i in $(seq 1 "${NUM_WORKERS}"); do
  echo "  -> launching worker #${i}"
  "${BIN_DIR}/mrworker" \
    -coord-addr="${COORD_ADDR}" \
    -storage="s3" \
    -s3-bucket="${BUCKET}" \
    -s3-input-prefix="${S3_INPUT_PREFIX}" \
    -s3-concurrency="${S3_CONCURRENCY}" \
    -idle-wait="${IDLE_WAIT}" \
    -log-level="${LOG_LEVEL}" \
    -app="${PLUGINS_DIR}/${APP_SO}" &
done

echo "*** All ${NUM_WORKERS} workers started in background."
echo "*** Use `ps aux | grep mrworker` or similar to inspect them."