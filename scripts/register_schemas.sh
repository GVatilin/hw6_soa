#!/bin/sh
set -eu

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
SUBJECT="${SUBJECT:-warehouse-events-value}"

until curl -fsS "$SCHEMA_REGISTRY_URL/subjects" >/dev/null; do
  sleep 2
done

curl -fsS -X PUT "$SCHEMA_REGISTRY_URL/config/$SUBJECT" \
  -H "Content-Type: application/json" \
  -d '{"compatibility":"BACKWARD"}'

register_schema() {
  schema_file="$1"
  schema_json="$(tr -d '\n' < "$schema_file" | sed 's/"/\\"/g')"
  curl -fsS -X POST "$SCHEMA_REGISTRY_URL/subjects/$SUBJECT/versions" \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "{\"schemaType\":\"AVRO\",\"schema\":\"$schema_json\"}"
}

for schema_file in $(find /schemas -maxdepth 1 -type f -name 'warehouse_event_v*.avsc' | sort -V); do
  register_schema "$schema_file"
done