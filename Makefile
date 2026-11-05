SHELL := /bin/bash
.SHELLFLAGS := -o pipefail -c

.PHONY: install check test build up
install:
	devbox run "bun install --frozen-lockfile"

check:
	devbox run "bun run check"

test:
	devbox run "bun test"

build:
	devbox run "bun run build"

ifneq ($(filter up,$(MAKECMDGOALS)),)
CLI_ARGS := $(filter-out up,$(MAKECMDGOALS))
%:
	@:
endif

up:
	devbox run --env-file .env "bun src/index.ts $(CLI_ARGS) $(ARGS)"
