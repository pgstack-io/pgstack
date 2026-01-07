sh:
	devbox --env-file .env shell

install:
	devbox run "cd src/common && go mod tidy && \
		cd ../syncer-postgres && go mod tidy && cd ../lib && go mod tidy && \
		cd ../../syncer-amplitude && go mod tidy && cd ./lib && go mod tidy && \
		cd ../../syncer-attio && go mod tidy && cd ./lib && go mod tidy && \
		cd ../../syncer-dialpad && go mod tidy && cd ./lib && go mod tidy && \
		cd ../../server && go mod tidy"

lint:
	devbox run "cd src/common && go fmt && staticcheck . && \
		cd ../syncer-postgres && go fmt && deadcode . && staticcheck . && cd ./lib && go fmt && staticcheck . && \
		cd ../../syncer-amplitude && go fmt && deadcode . && staticcheck . && cd ./lib && go fmt && staticcheck . && \
		cd ../../syncer-attio && go fmt && deadcode . && staticcheck . && cd ./lib && go fmt && staticcheck . && \
		cd ../../syncer-dialpad && go fmt && deadcode . && staticcheck . && cd ./lib && go fmt && staticcheck . && \
		cd ../../server && go fmt && deadcode . && staticcheck ."

build:
	./scripts/build-docker.sh

publish:
	./scripts/publish-docker.sh

server:
	docker run -it --rm --env-file .env  -p 54321:54321 ghcr.io/bemihq/bemidb:latest server

syncer-postgres:
	docker run -it --rm --env-file .env -e DESTINATION_SCHEMA_NAME=postgres ghcr.io/bemihq/bemidb:latest syncer-postgres

local-build:
	docker build --build-arg PLATFORM=linux/arm64 -t bemidb:local .

local-server: local-build
	docker run -it --rm --env-file .env -p 54321:54321 bemidb:local server

local-syncer-postgres: local-build
	docker run -it --rm --env-file .env -e DESTINATION_SCHEMA_NAME=postgres bemidb:local syncer-postgres

local-syncer-amplitude: local-build
	docker run -it --rm --env-file .env -e DESTINATION_SCHEMA_NAME=amplitude bemidb:local syncer-amplitude

local-syncer-attio: local-build
	docker run -it --rm --env-file .env -e DESTINATION_SCHEMA_NAME=attio bemidb:local syncer-attio

local-sh:
	docker run -it --rm --env-file .env bemidb:local bash

test-build:
	docker build --build-arg PLATFORM=linux/arm64 -t bemidb:test -f Dockerfile.test .

test: build-test
	docker run -it --rm bemidb:test

test-function:
	devbox run "cd src/server && go test ./... -run $(FUNC)"

debug:
	devbox run "cd src/server && dlv test github.com/BemiHQ/BemiDB"

console:
	devbox run "cd src/server && gore"

outdated:
	devbox run "cd src/server && go list -u -m -f '{{if and .Update (not .Indirect)}}{{.}}{{end}}' all"

.PHONY: benchmark
benchmark:
	devbox run "time psql postgres://127.0.0.1:54321/bemidb < ./benchmark/queries.sql"

pg-init:
	devbox run initdb && \
		sed -i "s/#log_statement = 'none'/log_statement = 'all'/g" ./.devbox/virtenv/postgresql/data/postgresql.conf && \
		sed -i "s/#logging_collector = off/logging_collector = on/g" ./.devbox/virtenv/postgresql/data/postgresql.conf && \
		sed -i "s/#log_directory = 'log'/log_directory = 'log'/g" ./.devbox/virtenv/postgresql/data/postgresql.conf

pg-up:
	devbox services start postgresql

pg-create:
	devbox run "(dropdb tpch || true) && \
		createdb tpch && \
		./benchmark/scripts/load-pg-data.sh"

pg-index:
	devbox run "psql postgres://127.0.0.1:5432/tpch -f ./benchmark/data/create-indexes.ddl"

pg-benchmark:
	devbox run "psql postgres://127.0.0.1:5432/tpch -c 'ANALYZE VERBOSE' && \
		time psql postgres://127.0.0.1:5432/tpch < ./benchmark/queries.sql"

pg-down:
	devbox services stop postgresql

pg-logs:
	tail -f .devbox/virtenv/postgresql/data/log/postgresql-*.log

pg-sniff:
	sudo tshark -i lo0 -f 'tcp port 5432' -d tcp.port==5432,pgsql -O pgsql

tpch-install:
	devbox run "cd benchmark && \
		rm -rf tpch-kit && \
		git clone https://github.com/gregrahn/tpch-kit.git && \
		cd tpch-kit/dbgen && \
		make MACHINE=$$MACHINE DATABASE=POSTGRESQL"

tpch-generate:
	devbox run "./benchmark/scripts/generate-data.sh"

sniff:
	sudo tshark -i lo0 -f 'tcp port 54321' -d tcp.port==54321,pgsql -O pgsql

measure-mem:
	devbox run "./benchmark/scripts/measure-memory.sh"

profile-mem:
	devbox run "watch -n 1 go tool pprof -top http://localhost:6060/debug/pprof/heap"
