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

post_event() {
  curl -sS -X POST "$WMS_URL/events" -H 'Content-Type: application/json' -d "$1" | pretty_print
}
post_event '{"event_id":"ooo-001","event_type":"PRODUCT_RECEIVED","product_id":"SKU-004","zone_id":"ZONE-A","quantity":100,"event_timestamp":"2026-04-01T12:00:00Z"}'
post_event '{"event_id":"ooo-002","event_type":"PRODUCT_SHIPPED","product_id":"SKU-004","zone_id":"ZONE-A","quantity":20,"event_timestamp":"2026-04-01T12:05:00Z"}'
post_event '{"event_id":"ooo-003","event_type":"PRODUCT_RECEIVED","product_id":"SKU-004","zone_id":"ZONE-A","quantity":50,"event_timestamp":"2026-04-01T12:02:00Z"}'

echo "Check: available should remain 80"
echo "curl -s http://localhost:8002/inventory/product/SKU-004/zone/ZONE-A | jq ."
