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

Release tags use `v<version>`, matching `package.json`. The release workflow builds standalone binaries and checksums, then creates a draft GitHub Release. The release title is the tag, such as `v2.0.0`. Review and publish the draft to make it available to the installer. Pushes to `main` run checks without creating a release. Build macOS binaries on macOS and sign/notarize them if distributing with an Apple Developer identity; regenerate checksums after signing.

Contributions are provided under MIT.
