#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_FILE="${CONNECTOR_FILE:-docker/cdc/connectors/mysql-loopers-connector.json}"

if [ ! -f "${CONNECTOR_FILE}" ]; then
  echo "connector file not found: ${CONNECTOR_FILE}" >&2
  exit 1
fi

echo "[1/2] Register connector from ${CONNECTOR_FILE}"
curl -sS -X POST \
  -H "Content-Type: application/json" \
  --data @"${CONNECTOR_FILE}" \
  "${CONNECT_URL}/connectors" || true

CONNECTOR_NAME="$(python3 - <<'PY'
import json
import os
path = os.environ.get("CONNECTOR_FILE", "docker/cdc/connectors/mysql-loopers-connector.json")
with open(path, "r", encoding="utf-8") as f:
    print(json.load(f)["name"])
PY
)"

echo "[2/2] Connector status: ${CONNECTOR_NAME}"
curl -sS "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status"
echo
