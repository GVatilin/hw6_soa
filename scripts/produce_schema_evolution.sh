#!/usr/bin/env bash
set -euo pipefail
WMS_URL="${WMS_URL:-http://localhost:8000}"

pretty_print() {
  if command -v jq >/dev/null 2>&1; then
    jq .
  else
    cat
  fi
}

curl -sS -X POST "$WMS_URL/events?schema_version=1" \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"schema-v1-001","event_type":"PRODUCT_RECEIVED","product_id":"SKU-SCHEMA-1","zone_id":"ZONE-A","quantity":10,"event_timestamp":"2026-04-01T15:00:00Z"}' | pretty_print

curl -sS -X POST "$WMS_URL/events?schema_version=2" \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"schema-v2-001","event_type":"PRODUCT_RECEIVED","product_id":"SKU-SCHEMA-2","zone_id":"ZONE-A","quantity":10,"supplier_id":"SUP-001","event_timestamp":"2026-04-01T15:01:00Z"}' | pretty_print

echo "Check supplier_id:"
echo "curl -s http://localhost:8002/inventory/product/SKU-SCHEMA-1/zone/ZONE-A | jq ."
echo "curl -s http://localhost:8002/inventory/product/SKU-SCHEMA-2/zone/ZONE-A | jq ."
