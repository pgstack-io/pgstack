#!/usr/bin/env bash
set -Eeuo pipefail
trap 'echo "Smoke test failed at line ${LINENO}: ${BASH_COMMAND}" >&2' ERR
IMAGE="${1:-pgstack:test}"
NAME="pgstack-smoke-${RANDOM}-${RANDOM}"
SOURCE="${NAME}-source"
APP="${NAME}-app"
CONFIG="$(mktemp)"
PASSWORD=smoke
docker network create "$NAME" >/dev/null
# Invoked by the EXIT trap.
# shellcheck disable=SC2329
cleanup() {
  local result=$?
  if (( result != 0 )); then docker logs "$APP" 2>&1 || true; fi
  docker rm -f "$APP" "$SOURCE" >/dev/null 2>&1 || true
  docker network rm "$NAME" >/dev/null 2>&1 || true
  rm -f "$CONFIG"
}
trap 'cleanup' EXIT

docker run -d --name "$SOURCE" --network "$NAME" \
  -e POSTGRES_PASSWORD=smoke postgres:17 -c wal_level=logical >/dev/null
for _ in {1..60}; do
  if docker exec "$SOURCE" pg_isready -U postgres >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec "$SOURCE" psql -U postgres -v ON_ERROR_STOP=1 \
  -c 'CREATE TABLE products (id int PRIMARY KEY, name text)' >/dev/null

docker run -d --name "$APP" --network "$NAME" \
  -e DATABASE_URL="postgres://postgres:smoke@${SOURCE}:5432/postgres" \
  -e PGSTACK_PASSWORD=smoke "$IMAGE" >/dev/null
query() {
  docker exec -e PGPASSWORD="$PASSWORD" -e PGCONNECT_TIMEOUT=3 "$SOURCE" \
    psql -X -At -h "$APP" -p 54321 -U reader -d pgstack -v ON_ERROR_STOP=1 -c "$1"
}
for _ in {1..120}; do
  if query 'SELECT 1' >/dev/null 2>&1; then break; fi
  sleep 1
done
[[ "$(query 'SELECT 1')" == 1 ]]
if docker exec -e PGPASSWORD=wrong "$SOURCE" \
  psql -X -h "$APP" -p 54321 -U reader -d pgstack -c 'SELECT 1' >/dev/null 2>&1; then
  echo 'Incorrect password was accepted' >&2
  exit 1
fi
for _ in {1..60}; do
  if [[ "$(docker exec "$SOURCE" psql -U postgres -At -c "SELECT active FROM pg_replication_slots WHERE slot_name='pgstack_local'")" == t ]]; then break; fi
  sleep 1
done
docker exec "$SOURCE" psql -U postgres -v ON_ERROR_STOP=1 \
  -c "INSERT INTO products VALUES (1, 'smoke jacket')" >/dev/null
AUDIT_CAPTURED=false
for _ in {1..90}; do
  if [[ "$(query "SELECT after->>'name' FROM audit.changes WHERE \"table\"='products'" 2>/dev/null || true)" == 'smoke jacket' ]]; then
    AUDIT_CAPTURED=true
    break
  fi
  sleep 1
done
[[ "$AUDIT_CAPTURED" == true ]] || { echo 'Audit record did not arrive' >&2; exit 1; }
docker stop -t 30 "$APP" >/dev/null
[[ "$(docker exec "$SOURCE" psql -U postgres -At -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name='pgstack_local'")" == 0 ]]
docker rm "$APP" >/dev/null

# Search-only quickstart: no query password and no embedding API key.
cat >"$CONFIG" <<'YAML'
audit:
  enabled: false
search:
  enabled: true
  tables:
    - name: public.products
      indexColumns: [name]
      storeColumns: [id, name]
YAML
chmod 644 "$CONFIG"
PASSWORD=""
docker run -d --name "$APP" --network "$NAME" \
  -e DATABASE_URL="postgres://postgres:smoke@${SOURCE}:5432/postgres" \
  --mount "type=bind,src=$CONFIG,dst=/app/pgstack.yaml,readonly" "$IMAGE" >/dev/null
for _ in {1..120}; do
  if [[ "$(query "SELECT name FROM search.public_products ORDER BY keyword_rank('jacket') LIMIT 1" 2>/dev/null || true)" == 'smoke jacket' ]]; then break; fi
  sleep 1
done
[[ "$(query "SELECT name FROM search.public_products ORDER BY keyword_rank('jacket') LIMIT 1")" == 'smoke jacket' ]]
[[ "$(docker exec "$APP" psql -X -At -c 'SELECT 1')" == 1 ]]
if query "SELECT name FROM search.public_products ORDER BY semantic_rank('jacket') LIMIT 1" >/dev/null 2>&1; then
  echo 'Semantic search unexpectedly accepted a keyword-only index' >&2
  exit 1
fi
docker exec "$SOURCE" psql -U postgres -v ON_ERROR_STOP=1 \
  -c "UPDATE products SET name='smoke boots' WHERE id=1" >/dev/null
for _ in {1..90}; do
  if [[ "$(query "SELECT name FROM search.public_products ORDER BY keyword_rank('boots') LIMIT 1" 2>/dev/null || true)" == 'smoke boots' ]]; then break; fi
  sleep 1
done
[[ "$(query "SELECT name FROM search.public_products ORDER BY keyword_rank('boots') LIMIT 1")" == 'smoke boots' ]]
docker stop -t 30 "$APP" >/dev/null
[[ "$(docker exec "$SOURCE" psql -U postgres -At -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name='pgstack_local'")" == 0 ]]
echo 'Container smoke test passed: SCRAM, passwordless local SQL, Audit, keyword-only Search, updates, slot cleanup'
