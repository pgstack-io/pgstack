# Security

Please report suspected vulnerabilities privately to **hi@pgstack.io**, with a reproduction and the affected version. Do not post credentials or exploit details in a public issue before coordination.

The bundled image is for a trusted local development machine. Bind its SQL port to `127.0.0.1`. Local connections are passwordless by default. Set `PGSTACK_PASSWORD` to require SCRAM-SHA-256 authentication, especially when publishing a port. It does not provide TLS, tenant isolation, or a sandbox for untrusted SQL. Source database credentials and the OpenAI key must remain private.

Use current PgStack releases; BemiDB 1.x deployment instructions do not describe this runtime.
