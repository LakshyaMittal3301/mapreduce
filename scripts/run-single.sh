#!/usr/bin/env bash

set -euo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 <app-name-or-go-file>   (e.g. wc or wc.go)"
    exit 1
fi

NREDUCE=10
JOB_ID="test"
COORD_ADDR="localhost:8123"
COORD_LISTEN=":8123"
COORD_PORT="8123"
LOG_LEVEL="${LOG_LEVEL:-info}"

COORDINATOR_ARGS=(-n-reduce="${NREDUCE}" -job-id="${JOB_ID}" -listen="${COORD_LISTEN}" -log-level="${LOG_LEVEL}")
WORKER_ARGS=(-coord-addr="${COORD_ADDR}" -log-level="${LOG_LEVEL}")

kill_port_listener() {
  local pids
  pids=$(lsof -ti tcp:"${COORD_PORT}" 2>/dev/null || true)
  if [ -n "$pids" ]; then
    kill $pids 2>/dev/null || true
  fi
}

cleanup() {
  local jobs_pids
  jobs_pids=$(jobs -p 2>/dev/null || true)
  if [ -n "$jobs_pids" ]; then
    kill $jobs_pids 2>/dev/null || true
  fi
  kill_port_listener
}

trap cleanup EXIT INT TERM
kill_port_listener

latest_output_dir() {
  ls -td job/*/output 2>/dev/null | head -n1
}

APP_ARG="$1"
APP_BASE="${APP_ARG%.go}"       # wc.go -> wc, wc -> wc
APP_GO="${APP_BASE}.go"         # wc -> wc.go
APP_SO="${APP_BASE}.so"         # wc -> wc.so

# We assume this script is run from map_reduce/scripts
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${ROOT_DIR}/bin"
PLUGIN_DIR="${BIN_DIR}/plugins"
TMP_ROOT="${ROOT_DIR}/tmp"
WORKDIR="${TMP_ROOT}/mr-single"

rm -rf "$WORKDIR"
mkdir -p "$WORKDIR" "$BIN_DIR" "$PLUGIN_DIR"
cd "$WORKDIR"

echo "*** Building plugin for ${APP_GO}"
(
  cd "${ROOT_DIR}/apps"
  go build -buildmode=plugin -o "${PLUGIN_DIR}/${APP_SO}" "${APP_GO}"
)

echo "*** Building coordinator/worker/sequential"
go build -o "${BIN_DIR}/mrcoordinator" "${ROOT_DIR}/cmd/mrcoordinator.go"
go build -o "${BIN_DIR}/mrworker"      "${ROOT_DIR}/cmd/mrworker.go"
go build -o "${BIN_DIR}/mrsequential"  "${ROOT_DIR}/cmd/mrsequential.go"

echo "*** Generating expected output with mrsequential"
"${BIN_DIR}/mrsequential" \
  "${PLUGIN_DIR}/${APP_SO}" \
  "${ROOT_DIR}/data/pg/pg"*".txt"

# if mrsequential writes mr-expected directly:
if [ -f mr-expected ]; then
    sort mr-expected > mr-expected.sorted
    mv mr-expected.sorted mr-expected
# otherwise, fall back to lab's default mr-out-0
elif [ -f mr-out-0 ]; then
    sort mr-out-0 > mr-expected
    rm -f mr-out-*
else
    echo "ERROR: mrsequential did not produce mr-expected or mr-out-0"
    exit 1
fi

echo "*** Running distributed MapReduce for ${APP_BASE}"

# start coordinator
"${BIN_DIR}/mrcoordinator" "${COORDINATOR_ARGS[@]}" "${ROOT_DIR}/data/pg/pg"*".txt" &
CID=$!

sleep 1

# start a few workers
"${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/${APP_SO}" &
"${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/${APP_SO}" &
"${BIN_DIR}/mrworker" "${WORKER_ARGS[@]}" -app="${PLUGIN_DIR}/${APP_SO}" &

wait "$CID" || true
wait || true

echo "*** Collecting and comparing output"
OUTPUT_DIR=$(latest_output_dir || true)
if [ -n "$OUTPUT_DIR" ] && ls "${OUTPUT_DIR}"/mr-out-* >/dev/null 2>&1; then
    sort "${OUTPUT_DIR}"/mr-out-* > mr-all
    if diff -u mr-expected mr-all >/dev/null; then
        echo "RESULT: PASS (${APP_BASE})"
        exit 0
    else
        echo "RESULT: FAIL (${APP_BASE})"
        echo "--- diff between expected and actual:"
        diff -u mr-expected mr-all || true
        exit 1
    fi
else
    echo "RESULT: FAIL (${APP_BASE}) - no job/*/output/mr-out-* files produced"
    exit 1
fi
