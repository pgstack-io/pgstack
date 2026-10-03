IMAGE ?= ghcr.io/pgstack-io/pgstack:local

.PHONY: build test lint
build:
	docker build -t $(IMAGE) .

test:
	devbox run "cd cdc && go test ./... && cd ../processor && go test ./... && cd ../server && go test ./..."

lint:
	devbox run "cd cdc && go vet ./... && cd ../processor && go vet ./... && cd ../server && go vet ./..."

.PHONY: test-config test-container
test-config:
	python3 -m unittest discover -p 'test_local_config.py'

test-container:
	bash tests/smoke.sh "$(IMAGE)"
