# Contributing

Bug reports and focused pull requests are welcome. Include a minimal reproduction, source Postgres version, PgStack version, and redacted logs. Never include database credentials, API keys, or customer data.

The repository contains three Go modules:

- `cdc`: Postgres logical replication and initial Search snapshots.
- `processor`: Audit storage and Search indexing.
- `server`: SQL compatibility, query execution, and authentication.

Use `make test` and `make lint` with Devbox, then `make build` to check the complete local image. Add regression tests for behavior changes. Keep fixtures synthetic. Document public configuration changes in the README and preserve the simple local quickstart.

BemiDB 1.x history remains available in Git. PgStack 2.x is a new runtime and configuration; do not assume compatibility with old deployment instructions or storage layouts.

Contributions are provided under the repository's AGPL-3.0 license.
