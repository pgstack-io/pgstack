ARG PLATFORM=linux/arm64

FROM --platform=$PLATFORM debian:12-slim AS base

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  curl ca-certificates \
  postgresql-client \
  gcc g++ libc6-dev \
  && rm -rf /var/lib/apt/lists/*

RUN adduser --disabled-login app

WORKDIR /app

COPY --chown=app:app scripts/catalog.sql /app/scripts/

# Set up syncers and server ########################################################################

FROM base AS compile

# Install Go

ENV GOROOT /usr/local/go
ENV GOPATH /go
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH

RUN \
  ARCH=$(dpkg --print-architecture) \
  && curl -L "https://go.dev/dl/go1.24.4.linux-$ARCH.tar.gz" -o go.tar.gz \
  && tar -C /usr/local -xzf go.tar.gz \
  && rm go.tar.gz \
  && mkdir -p "$GOPATH/src" "$GOPATH/bin" \
  && chmod -R 777 "$GOPATH"

# Compile syncers and server

COPY --chown=app:app src/common/go.mod src/common/go.sum /app/src/common/

COPY --chown=app:app src/syncer-postgres/lib/go.mod src/syncer-postgres/lib/go.sum /app/src/syncer-postgres/lib/
COPY --chown=app:app src/syncer-postgres/go.mod src/syncer-postgres/go.sum /app/src/syncer-postgres/
RUN cd /app/src/syncer-postgres && go mod download

COPY --chown=app:app src/syncer-amplitude/lib/go.mod src/syncer-amplitude/lib/go.sum /app/src/syncer-amplitude/lib/
COPY --chown=app:app src/syncer-amplitude/go.mod src/syncer-amplitude/go.sum /app/src/syncer-amplitude/
RUN cd /app/src/syncer-amplitude && go mod download

COPY --chown=app:app src/syncer-attio/lib/go.mod src/syncer-attio/lib/go.sum /app/src/syncer-attio/lib/
COPY --chown=app:app src/syncer-attio/go.mod src/syncer-attio/go.sum /app/src/syncer-attio/
RUN cd /app/src/syncer-attio && go mod download

COPY --chown=app:app src/syncer-dialpad/lib/go.mod src/syncer-dialpad/lib/go.sum /app/src/syncer-dialpad/lib/
COPY --chown=app:app src/syncer-dialpad/go.mod src/syncer-dialpad/go.sum /app/src/syncer-dialpad/
RUN cd /app/src/syncer-dialpad && go mod download

COPY --chown=app:app src/server/go.mod src/server/go.sum /app/src/server/
RUN cd /app/src/server && go mod download

COPY --chown=app:app src/common /app/src/common
COPY --chown=app:app src/syncer-postgres /app/src/syncer-postgres
COPY --chown=app:app src/syncer-amplitude /app/src/syncer-amplitude
COPY --chown=app:app src/syncer-attio /app/src/syncer-attio
COPY --chown=app:app src/syncer-dialpad /app/src/syncer-dialpad
COPY --chown=app:app src/server /app/src/server

RUN ARCH=$(dpkg --print-architecture) \
  && cd /app/src/syncer-postgres && CGO_ENABLED=1 GOOS=linux GOARCH=$ARCH go build -o /app/bin/syncer-postgres \
    && cd /app/src/syncer-amplitude && CGO_ENABLED=1 GOOS=linux GOARCH=$ARCH go build -o /app/bin/syncer-amplitude \
    && cd /app/src/syncer-attio && CGO_ENABLED=1 GOOS=linux GOARCH=$ARCH go build -o /app/bin/syncer-attio \
    && cd /app/src/syncer-dialpad && CGO_ENABLED=1 GOOS=linux GOARCH=$ARCH go build -o /app/bin/syncer-dialpad \
    && cd /app/src/server && CGO_ENABLED=1 GOOS=linux GOARCH=$ARCH go build -o /app/bin/server

# Prepare final image ##############################################################################

FROM base AS final

COPY --chown=app:app --from=compile \
  /app/bin/syncer-postgres \
  /app/bin/syncer-amplitude \
  /app/bin/syncer-attio \
  /app/bin/syncer-dialpad \
  /app/bin/server \
  /app/bin/
COPY --chown=app:app docker/bin /app/bin/

USER app

ENTRYPOINT ["/app/bin/run.sh"]
