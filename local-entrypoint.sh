#!/usr/bin/env bash

set -Eeuo pipefail

readonly LOCAL_DATA_DIR="/var/lib/pgstack"
readonly LOCAL_NATS_DIR="${LOCAL_DATA_DIR}/nats"
readonly LOCAL_S3_DIR="${LOCAL_DATA_DIR}/s3"
readonly LOCAL_NATS_CONFIG="/tmp/pgstack-local-nats.conf"

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "DATABASE_URL is required (for example: postgres://postgres:postgres@host.docker.internal:5432/postgres)" >&2
  exit 1
fi

# Validate user configuration before touching local state or source Postgres.
LOCAL_CONFIG="$(python3 /app/local_config.py /app/pgstack.yaml)"
unset AUDIT_INCLUDED_TABLES AUDIT_EXCLUDED_TABLES AUDIT_IGNORE_CHANGE_COLUMNS
AUDIT_ENABLED="$(jq -r '.audit.enabled' <<<"$LOCAL_CONFIG")"
SEARCH_TABLES_JSON="$(jq -c '.search.tables' <<<"$LOCAL_CONFIG")"
SEARCH_TABLES="$(jq -r '.search.tables | map(.name) | join(",")' <<<"$LOCAL_CONFIG")"
SEARCH_SNAPSHOT="$(jq -r '.search.enabled' <<<"$LOCAL_CONFIG")"
export AUDIT_ENABLED SEARCH_TABLES_JSON SEARCH_TABLES SEARCH_SNAPSHOT
export SEARCH_BASE_PATH="local/search"
if [[ "$AUDIT_ENABLED" == true ]]; then
  # An explicitly empty exclusion list enables Audit for every table.
  export AUDIT_EXCLUDED_TABLES=""
fi
if [[ "$SEARCH_SNAPSHOT" == true && -z "${OPENAI_API_KEY:-}" ]]; then
  SEARCH_TABLES_JSON="$(jq -c 'map(. + {keywordOnly: true})' <<<"$SEARCH_TABLES_JSON")"
  export SEARCH_TABLES_JSON
fi

# Each start creates a fresh local capture.
rm -rf "${LOCAL_NATS_DIR}" "${LOCAL_S3_DIR}"
mkdir -p "${LOCAL_NATS_DIR}" "${LOCAL_S3_DIR}"
printf '%s\n' 'max_payload: 2MB' >"${LOCAL_NATS_CONFIG}"

# CDC configuration
export SLOT_NAME="pgstack_local"
export PUBLICATION_NAME="pgstack"
export NATS_URL="nats://127.0.0.1:4222"
export NATS_STREAM_NAME="pgstack"
export NATS_SUBJECT="pgstack.changes"

# Local S3/Iceberg configuration
export AWS_S3_ENDPOINT="http://127.0.0.1:9000"
export AWS_ACCESS_KEY_ID="pgstack"
export AWS_SECRET_ACCESS_KEY="pgstack"
export AWS_REGION="us-east-1"
export AWS_S3_BUCKET="pgstack"
export AUDIT_BASE_PATH="local/audit"
export NATS_CONSUMER_NAME="pgstack-processor"

# Local-edition product limits. These are intentionally not configurable.
export RETENTION_DAYS="1"
export MAX_HOT_PARQUET_SIZE_MB="10"
export NATS_BATCH_INTERVAL_SEC="10"
export DUCKDB_MEMORY_LIMIT="512MB"

# PostgreSQL-compatible read endpoint
export PGSTACK_HOST="0.0.0.0"
export PGSTACK_PORT="54321"
export PGSTACK_DATABASE="pgstack"
export PGSTACK_USER=""
export PGSTACK_PASSWORD="${PGSTACK_PASSWORD:-}"
export LOG_LEVEL="WARN"

declare -a PROCESS_PIDS=()
declare -A PROCESS_NAMES=()

start_process() {
  local name="$1"
  shift

  "$@" &
  local pid=$!
  PROCESS_PIDS+=("${pid}")
  PROCESS_NAMES["${pid}"]="${name}"
  echo "Started ${name} (pid=${pid})"
}

start_process_without_stdout() {
  local name="$1"
  shift

  "$@" >/dev/null &
  local pid=$!
  PROCESS_PIDS+=("${pid}")
  PROCESS_NAMES["${pid}"]="${name}"
  echo "Started ${name} (pid=${pid})"
}

filter_nats_stderr() {
  local line

  while IFS= read -r line; do
    if [[ "${line}" != *"[INF]"* ]]; then
      printf '%s\n' "${line}" >&2
    fi
  done
}

filter_rclone_stderr() {
  local data_cleanup_error=" ERROR : ${AWS_S3_BUCKET}/${AUDIT_BASE_PATH}/changes/data/: Dir.Remove not empty"
  local metadata_cleanup_error=" ERROR : ${AWS_S3_BUCKET}/${AUDIT_BASE_PATH}/changes/metadata/: Dir.Remove not empty"
  local line

  while IFS= read -r line; do
    if [[ "${line}" != *"${data_cleanup_error}" && "${line}" != *"${metadata_cleanup_error}" ]]; then
      printf '%s\n' "${line}" >&2
    fi
  done
}

wait_for_port() {
  local host="$1"
  local port="$2"
  local name="$3"

  for _ in {1..300}; do
    if (echo >/dev/tcp/"${host}"/"${port}") >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done

  echo "${name} did not become ready on ${host}:${port}" >&2
  return 1
}

prepare_source_postgres() {
  local active_pid
  active_pid="$(
    psql "${DATABASE_URL}" \
      --no-psqlrc \
      --tuples-only \
      --no-align \
      --set=ON_ERROR_STOP=1 \
      --command="SELECT active_pid FROM pg_replication_slots WHERE slot_name = 'pgstack_local' AND active"
  )"

  if [[ -n "${active_pid}" ]]; then
    echo "Replication slot ${SLOT_NAME} is already active on backend ${active_pid}" >&2
    return 1
  fi

  psql "${DATABASE_URL}" \
    --no-psqlrc \
    --set=ON_ERROR_STOP=1 <<'SQL' >/dev/null
SELECT 'CREATE PUBLICATION pgstack FOR ALL TABLES'
WHERE NOT EXISTS (
  SELECT 1
  FROM pg_publication
  WHERE pubname = 'pgstack'
)
\gexec

SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE slot_name = 'pgstack_local';
SQL
}

setup_nats_stream() {
  for _ in {1..300}; do
    if nats --server "${NATS_URL}" --no-context stream add "${NATS_STREAM_NAME}" \
      --subjects "${NATS_SUBJECT}" \
      --storage file \
      --retention work \
      --discard old \
      --max-age 24h \
      --dupe-window 24h \
      --replicas 1 \
      --allow-direct \
      --deny-delete \
      --defaults >/dev/null 2>&1; then
      echo "NATS JetStream is ready: stream=${NATS_STREAM_NAME} subject=${NATS_SUBJECT}"
      return 0
    fi
    sleep 0.1
  done

  echo "Failed to create local NATS stream" >&2
  return 1
}

# Called by the EXIT trap through cleanup.
# shellcheck disable=SC2317,SC2329
drop_local_replication_slot() {
  if ! psql "${DATABASE_URL}" \
    --no-psqlrc \
    --quiet \
    --set=ON_ERROR_STOP=1 \
    --command="SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'pgstack_local' AND NOT active" >/dev/null; then
    echo "Warning: failed to drop local replication slot ${SLOT_NAME}" >&2
  fi
}

# Registered as the EXIT trap after source preparation.
# shellcheck disable=SC2317,SC2329
cleanup() {
  trap - EXIT INT TERM

  if ((${#PROCESS_PIDS[@]} > 0)); then
    kill -TERM "${PROCESS_PIDS[@]}" 2>/dev/null || true
    wait "${PROCESS_PIDS[@]}" 2>/dev/null || true
  fi

  drop_local_replication_slot
}

echo "PGStack local edition"

prepare_source_postgres

# Failed setup must never clean up another instance's replication slot.
trap 'cleanup' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

start_process_without_stdout "nats" nats-server \
  --config "${LOCAL_NATS_CONFIG}" \
  -js \
  -sd "${LOCAL_NATS_DIR}" \
  -a 127.0.0.1 \
  -p 4222 \
  2> >(filter_nats_stderr)
setup_nats_stream

start_process_without_stdout "s3" rclone serve s3 "${LOCAL_S3_DIR}" \
  --addr 127.0.0.1:9000 \
  --log-level ERROR \
  --no-checksum \
  --no-cleanup \
  2> >(filter_rclone_stderr)
wait_for_port 127.0.0.1 9000 "Local S3"

start_process "processor" /app/bin/processor
start_process "server" /app/bin/server
start_process "cdc" /app/bin/cdc

echo "PostgreSQL read endpoint: postgres://127.0.0.1:${PGSTACK_PORT}/${PGSTACK_DATABASE}?sslmode=disable"

set +e
exited_pid=""
wait -n -p exited_pid "${PROCESS_PIDS[@]}"
status=$?
set -e

if [[ -n "${exited_pid}" ]]; then
  echo "${PROCESS_NAMES[${exited_pid}]:-A child process} exited with status ${status}; stopping local stack" >&2
else
  echo "A child process exited with status ${status}; stopping local stack" >&2
fi

exit "${status}"
