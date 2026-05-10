#!/usr/bin/env bash
set -euo pipefail
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"

pretty_print() {
  if command -v jq >/dev/null 2>&1; then
    jq .
  else
    cat
  fi
}

echo "Compatibility:"
curl -sS "$SCHEMA_REGISTRY_URL/config/warehouse-events-value" | pretty_print
echo "Versions:"
versions="$(curl -sS "$SCHEMA_REGISTRY_URL/subjects/warehouse-events-value/versions")"
printf '%s\n' "$versions" | pretty_print

for version in $(printf '%s' "$versions" | tr -d '[],' ); do
  echo "Schema version $version:"
  curl -sS "$SCHEMA_REGISTRY_URL/subjects/warehouse-events-value/versions/$version" | pretty_print
done
