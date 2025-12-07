#!/usr/bin/env bash

set -euo pipefail

if [ $# -ne 3 ]; then
  echo "Usage: $0 <app-name-or-go-file> <s3-bucket> <job-id>"
  echo "Example: $0 wc my-mapreduce-bucket s3test-1733650000000"
  exit 1
fi

APP_ARG="$1"   # e.g. wc or wc.go
BUCKET="$2"
JOB_ID="$3"

APP_BASE="${APP_ARG%.go}"   # wc.go -> wc, wc -> wc
APP_GO="${APP_BASE}.go"
APP_SO="${APP_BASE}.so"

# repo root: .../map_reduce
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${ROOT_DIR}/bin"
PLUGINS_DIR="${BIN_DIR}/plugins"
TMP_ROOT="${ROOT_DIR}/tmp"
WORKDIR="${TMP_ROOT}/check-${APP_BASE}-${JOB_ID}"

mkdir -p "${WORKDIR}" "${BIN_DIR}" "${PLUGINS_DIR}"
cd "${WORKDIR}"

echo "*** Building plugin and mrsequential for ${APP_GO}"
(
  cd "${ROOT_DIR}/apps"
  go build -buildmode=plugin -o "${PLUGINS_DIR}/${APP_SO}" "${APP_GO}"
)

go build -o "${BIN_DIR}/mrsequential" "${ROOT_DIR}/cmd/mrsequential.go"

echo "*** Generating expected output locally with mrsequential"
"${BIN_DIR}/mrsequential" \
  "${PLUGINS_DIR}/${APP_SO}" \
  "${ROOT_DIR}/data/pg/pg-"*.txt

if [ -f mr-out-0 ]; then
  sort mr-out-0 > mr-expected
  rm -f mr-out-*
else
  echo "ERROR: mrsequential did not produce mr-out-0"
  exit 1
fi

echo "*** Downloading S3 outputs for job ${JOB_ID} from s3://${BUCKET}/jobs/${JOB_ID}/output/"
mkdir -p s3-output
aws s3 sync "s3://${BUCKET}/jobs/${JOB_ID}/output/" ./s3-output

if ! ls s3-output/mr-out-* >/dev/null 2>&1; then
  echo "ERROR: no mr-out-* files found in s3://${BUCKET}/jobs/${JOB_ID}/output/"
  exit 1
fi

echo "*** Comparing S3 output with local expected output"
sort s3-output/mr-out-* > mr-all

if diff -u mr-expected mr-all >/dev/null; then
  echo "S3 RESULT: PASS (${APP_BASE}, job ${JOB_ID})"
  exit 0
else
  echo "S3 RESULT: FAIL (${APP_BASE}, job ${JOB_ID})"
#   echo "--- diff between expected and actual:"
#   diff -u mr-expected mr-all || true
  exit 1
fi
