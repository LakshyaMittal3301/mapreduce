#!/usr/bin/env bash

set -euo pipefail

# Usage:
#   ./scripts/run-worker-ec2.sh <s3-bucket> <coord-addr>
#
# Example:
#   ./scripts/run-worker-ec2.sh mapreduce-bucket 10.0.1.23:8123
#
#   s3-bucket  : S3 bucket name (e.g. mapreduce-bucket)
#   coord-addr : coordinator address, e.g. 10.0.1.23:8123 or my-elastic-ip:8123
#   num-workers: always 1 (this script launches a single worker)

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <s3-bucket> <coord-addr>"
  echo "Example: $0 mapreduce-bucket 10.0.1.23:8123"
  exit 1
fi

BUCKET="$1"
COORD_ADDR="$2"

# ---- Defaults (overridable via env or by editing this script) ----
LOG_LEVEL="${LOG_LEVEL:-info}"
S3_CONCURRENCY="${S3_CONCURRENCY:-16}"
IDLE_WAIT="${IDLE_WAIT:-1s}"
# -----------------------------------------------------------------

# Root of the repo: .../map_reduce
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BIN_DIR="${ROOT_DIR}/bin"

mkdir -p "${BIN_DIR}"

echo "*** Building mrworker binary"
(
  cd "${ROOT_DIR}/cmd"
  go build -o "${BIN_DIR}/mrworker" mrworker.go
)

echo "*** Starting 1 worker"
echo "  coord addr     : ${COORD_ADDR}"
echo "  bucket         : ${BUCKET}"
echo "  log level      : ${LOG_LEVEL}"
echo "  S3 concurrency : ${S3_CONCURRENCY}"
echo "  idle wait      : ${IDLE_WAIT}"

echo "  -> launching worker"
"${BIN_DIR}/mrworker" \
  -coord-addr="${COORD_ADDR}" \
  -storage="s3" \
  -s3-bucket="${BUCKET}" \
  -s3-concurrency="${S3_CONCURRENCY}" \
  -idle-wait="${IDLE_WAIT}" \
  -log-level="${LOG_LEVEL}" &
WORKER_PID=$!

cleanup() {
  echo "*** Stopping worker (pid ${WORKER_PID})"
  kill "${WORKER_PID}" 2>/dev/null || true
  wait "${WORKER_PID}" 2>/dev/null || true
}

trap cleanup INT TERM EXIT

echo "*** Worker started in background."
echo "*** Use \`ps aux | grep mrworker\` or similar to inspect it."
wait "${WORKER_PID}"
