FROM golang:1.26-bookworm AS go-builder

WORKDIR /src

COPY cdc/ ./cdc/
COPY processor/ ./processor/
COPY server/ ./server/

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd cdc && CGO_ENABLED=1 go build -mod=readonly -trimpath -ldflags="-s -w" -o /out/cdc . && \
    cd ../processor && CGO_ENABLED=1 go build -mod=readonly -trimpath -ldflags="-s -w" -o /out/processor . && \
    cd ../server && CGO_ENABLED=1 go build -mod=readonly -trimpath -ldflags="-s -w" -o /out/server .

################################################################################

FROM nats:2.11.8 AS nats-server

################################################################################

FROM natsio/nats-box:0.19.7 AS nats-box

################################################################################

FROM rclone/rclone:1.74.2 AS rclone

################################################################################

FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates libstdc++6 postgresql-client tini jq python3-yaml && \
    rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 10001 app && \
    mkdir -p /app/bin /var/lib/pgstack && \
    chown -R app:app /app /var/lib/pgstack

COPY --from=nats-server /nats-server /usr/local/bin/nats-server
COPY --from=nats-box /usr/local/bin/nats /usr/local/bin/nats
COPY --from=rclone /usr/local/bin/rclone /usr/local/bin/rclone
COPY --from=go-builder /out/cdc /app/bin/cdc
COPY --from=go-builder /out/processor /app/bin/processor
COPY --from=go-builder /out/server /app/bin/server
COPY --chown=app:app local-entrypoint.sh /app/local-entrypoint.sh

COPY --chown=app:app local_config.py /app/local_config.py

USER app
WORKDIR /app

LABEL org.opencontainers.image.title="PgStack" \
      org.opencontainers.image.source="https://github.com/pgstack-io/pgstack" \
      org.opencontainers.image.licenses="AGPL-3.0"

ENV PGHOST=127.0.0.1 PGPORT=54321 PGDATABASE=pgstack PGUSER=pgstack

EXPOSE 54321

ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/app/local-entrypoint.sh"]
