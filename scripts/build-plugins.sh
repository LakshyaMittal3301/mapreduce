#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APPS_DIR="${ROOT_DIR}/apps"
PLUGINS_DIR="${ROOT_DIR}/bin/plugins"

mkdir -p "${PLUGINS_DIR}"

if [ "${CGO_ENABLED:-}" != "1" ]; then
  echo "WARN: CGO_ENABLED is not set to 1. Go plugins require CGO." >&2
fi

echo "*** Building plugins from ${APPS_DIR}"
for app_path in "${APPS_DIR}"/*.go; do
  app_file="$(basename "${app_path}")"
  app_name="${app_file%.go}"
  out_path="${PLUGINS_DIR}/${app_name}.so"
  echo "- ${app_file} -> ${out_path}"
  go build -buildmode=plugin -o "${out_path}" "${app_path}"
done

echo "*** Done. Plugins are in ${PLUGINS_DIR}"
