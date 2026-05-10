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

curl -sS -X POST "$WMS_URL/events" \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"dlq-001","event_type":"PRODUCT_SHIPPED","product_id":"SKU-005","zone_id":"ZONE-A","quantity":-5,"event_timestamp":"2026-04-01T14:00:00Z"}' | pretty_print

echo "Read DLQ: docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic warehouse-events-dlq --from-beginning --max-messages 1"
