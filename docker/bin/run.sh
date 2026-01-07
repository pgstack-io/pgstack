#!/bin/bash

set -euo pipefail

case "${1:-}" in
  bash)
    exec /bin/bash
    ;;
  syncer-postgres)
    : "${SOURCE_POSTGRES_DATABASE_URL:?Environment variable SOURCE_POSTGRES_DATABASE_URL must be set}"
    : "${DESTINATION_SCHEMA_NAME:?Environment variable DESTINATION_SCHEMA_NAME must be set}"

    psql $CATALOG_DATABASE_URL -f /app/scripts/catalog.sql

    echo "Starting Syncer for PostgreSQL..."
    SOURCE_POSTGRES_SYNC_MODE=FULL_REFRESH ./bin/syncer-postgres 2>&1 | sed 's/^/[Syncer] /'
    echo "Syncer for PostgreSQL finished."
    ;;
  syncer-amplitude)
    : "${SOURCE_AMPLITUDE_API_KEY:?Environment variable SOURCE_AMPLITUDE_API_KEY must be set}"
    : "${SOURCE_AMPLITUDE_SECRET_KEY:?Environment variable SOURCE_AMPLITUDE_SECRET_KEY must be set}"
    : "${DESTINATION_SCHEMA_NAME:?Environment variable DESTINATION_SCHEMA_NAME must be set}"

    psql $CATALOG_DATABASE_URL -f /app/scripts/catalog.sql

    echo "Starting Syncer for Amplitude..."
    ./bin/syncer-amplitude 2>&1 | sed 's/^/[Syncer] /'
    echo "Syncer for Amplitude finished."
    ;;
  syncer-attio)
    : "${SOURCE_ATTIO_API_ACCESS_TOKEN:?Environment variable SOURCE_ATTIO_API_ACCESS_TOKEN must be set}"
    : "${DESTINATION_SCHEMA_NAME:?Environment variable DESTINATION_SCHEMA_NAME must be set}"

    psql $CATALOG_DATABASE_URL -f /app/scripts/catalog.sql

    echo "Starting Syncer for Attio..."
    ./bin/syncer-attio 2>&1 | sed 's/^/[Syncer] /'
    echo "Syncer for Attio finished."
    ;;
  syncer-dialpad)
    : "${NATS_URL:?Environment variable NATS_URL must be set}"
    : "${NATS_JETSTREAM_STREAM:?Environment variable NATS_JETSTREAM_STREAM must be set}"
    : "${NATS_JETSTREAM_SUBJECT:?Environment variable NATS_JETSTREAM_SUBJECT must be set}"
    : "${NATS_JETSTREAM_CONSUMER_NAME:?Environment variable NATS_JETSTREAM_CONSUMER_NAME must be set}"
    : "${DESTINATION_SCHEMA_NAME:?Environment variable DESTINATION_SCHEMA_NAME must be set}"

    psql $CATALOG_DATABASE_URL -f /app/scripts/catalog.sql

    echo "Starting Syncer for Dialpad..."
    ./bin/syncer-dialpad 2>&1 | sed 's/^/[Syncer] /'
    echo "Syncer for Dialpad finished."
    ;;
  server)
    : "${AWS_REGION:?Environment variable AWS_REGION must be set}"
    : "${AWS_S3_BUCKET:?Environment variable AWS_S3_BUCKET must be set}"
    : "${AWS_ACCESS_KEY_ID:?Environment variable AWS_ACCESS_KEY_ID must be set}"
    : "${AWS_SECRET_ACCESS_KEY:?Environment variable AWS_SECRET_ACCESS_KEY must be set}"
    : "${CATALOG_DATABASE_URL:?Environment variable CATALOG_DATABASE_URL must be set}"

    echo "Starting server..."
    ./bin/server
    ;;
  *)
    echo "Unknown argument: ${1:-}"
    echo "Available options: syncer-postgres, bash"
    exit 1
    ;;
esac
