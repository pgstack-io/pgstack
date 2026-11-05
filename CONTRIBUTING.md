# Contributing

Bug reports and focused pull requests are welcome. Include the CLI version, operating system, steps to reproduce, and redacted output. Never include credentials or PostgreSQL connection URLs with passwords.

Use Devbox and Bun:

```sh
make install
make check
make test
make build
```

The CLI lives in `src/`. The API contract is in `src/api.ts`. Add regression coverage for behavior changes and keep fixtures synthetic. Keep the contract aligned with the supported PgStack API.

Release tags use `v<version>`, matching `package.json`. The release workflow builds standalone binaries, checksums, a version file, and the license, then creates a draft GitHub Release. Review and publish the draft to make it available to the installer. Build macOS binaries on macOS and sign/notarize them if distributing with an Apple Developer identity; regenerate checksums after signing.

Contributions are provided under MIT.
