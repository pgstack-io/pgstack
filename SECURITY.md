# Security

Report suspected vulnerabilities privately to **hi@pgstack.io**, with the CLI version and a minimal reproduction. Do not publish credentials or exploit details in an issue before coordination.

Only approve browser login requests you started, after matching the terminal's code. The CLI stores its bearer token in `~/.config/pgstack/credentials.json`; protect that file and any backups. Logout removes the local token but cannot invalidate copied tokens. Avoid sharing connection URLs, which contain temporary database credentials.

The installer verifies downloads against release SHA-256 files. This detects mismatched or corrupted downloads; it depends on the integrity of GitHub Releases and the installation script. Download only from PgStack's published installation URL or this repository's releases.
