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
  local payload="$1"
  curl -sS -X POST "$WMS_URL/events" \
    -H 'Content-Type: application/json' \
    -d "$payload" | pretty_print
}

post_event '{"event_id":"basic-001","event_type":"PRODUCT_RECEIVED","product_id":"SKU-001","zone_id":"ZONE-A","quantity":100,"event_timestamp":"2026-04-01T12:00:00Z"}'
post_event '{"event_id":"basic-002","event_type":"PRODUCT_RESERVED","product_id":"SKU-001","zone_id":"ZONE-A","quantity":30,"event_timestamp":"2026-04-01T12:01:00Z"}'
post_event '{"event_id":"basic-003","event_type":"PRODUCT_MOVED","product_id":"SKU-001","from_zone_id":"ZONE-A","to_zone_id":"ZONE-B","quantity":20,"event_timestamp":"2026-04-01T12:02:00Z"}'
post_event '{"event_id":"basic-004","event_type":"PRODUCT_SHIPPED","product_id":"SKU-001","zone_id":"ZONE-A","quantity":10,"event_timestamp":"2026-04-01T12:03:00Z"}'
post_event '{"event_id":"basic-005","event_type":"ORDER_CREATED","order_id":"ORDER-001","items":[{"product_id":"SKU-001","quantity":15}],"event_timestamp":"2026-04-01T12:04:00Z"}'
post_event '{"event_id":"basic-006","event_type":"ORDER_COMPLETED","order_id":"ORDER-001","event_timestamp":"2026-04-01T12:05:00Z"}'

echo "Check: curl -s http://localhost:8002/inventory/product/SKU-001 | jq ."
