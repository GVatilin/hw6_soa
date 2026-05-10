#!/usr/bin/env bash
set -euo pipefail
WMS_URL="${WMS_URL:-http://localhost:8000}"
PAYLOAD='{"event_id":"idem-001","event_type":"PRODUCT_RECEIVED","product_id":"SKU-002","zone_id":"ZONE-A","quantity":50,"event_timestamp":"2026-04-01T13:00:00Z"}'

pretty_print() {
  if command -v jq >/dev/null 2>&1; then
    jq .
  else
    cat
  fi
}

for i in 1 2; do
  curl -sS -X POST "$WMS_URL/events" -H 'Content-Type: application/json' -d "$PAYLOAD" | pretty_print
done

echo "Check: curl -s http://localhost:8002/inventory/product/SKU-002/zone/ZONE-A | jq ."
