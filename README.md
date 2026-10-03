# <img src="assets/pgstack-logo.png" alt="PgStack" width="420">

**PgStack** is Postgres read replicas for Search and Audit.

- **Search**: a search-optimized replica for semantic, keyword, and hybrid retrieval.
- **Audit**: a history of row changes for automatic data change tracking.

PgStack replicates your data while your existing Postgres remains the source of truth. Indexing and queries run outside your application database through a separate Postgres-compatible endpoint.

Run with one Docker command, or use [PgStack Cloud](https://pgstack.io/) for managed production workloads.

## Contents

- [Highlights](#highlights)
- [Use Cases](#use-cases)
- [Quickstart](#quickstart)
- [Usage](#usage)
  - [Audit](#audit)
  - [Search](#search)
  - [Configuration](#configuration)
- [Architecture](#architecture)
- [OSS vs Cloud](#oss-vs-cloud)
- [License](#license)

## Highlights

- **Specialized Replicas**: continuously synchronized, with Search and Audit isolated from OLTP workloads.
- **Easy Setup**: starts in a single container with your database URL.
- **Postgres Compatibility**: integrates with clients and tools in the Postgres ecosystem.
- **Open-Source**: released under the OSI-approved AGPL-3.0 license.

## Use Cases

- **Search**
  - **Product Search**: find relevant information across operational records and knowledge bases.
  - **AI and RAG Retrieval**: retrieve current application data as context for AI agents and assistants.
  - **Duplicate Detection**: surface related tickets, issues, and requests expressed in different words.
  - **Find Similar**: discover related companies, people, jobs, articles, and other application records.
- **Audit**
  - **Audit Trails and Compliance**: track record changes to support data audits and compliance reviews.
  - **Troubleshooting**: investigate unexpected data issues and understand how your data changed.
  - **Selective Data Recovery**: identify previous values to recover updated or deleted records.
  - **Customer Activity Feeds**: turn data changes into activity feeds in your product.

## Quickstart

Use a development Postgres database and a superuser connection for automatic setup. Check logical replication with:

```sql
SHOW wal_level;
```

If the result is not `logical`, enable it and restart Postgres:

```sql
ALTER SYSTEM SET wal_level = 'logical';
```

Start PgStack with your database URL:

```sh
docker run --rm --name pgstack \
  -p 54321:54321 \
  -e DATABASE_URL='postgres://postgres:postgres@host.docker.internal:5432/postgres' \
  ghcr.io/pgstack-io/pgstack:latest
```

On Docker Desktop, `host.docker.internal` points to your computer. On Linux Docker Engine, add `--add-host=host.docker.internal:host-gateway` to make that name resolve to the host gateway. For a remote database, use its reachable hostname. URL-encode reserved characters in the source password.

The image supports Linux amd64 and arm64, including Apple Silicon. `latest` tracks stable [releases](https://github.com/pgstack-io/pgstack/releases).

## Usage

### Audit

Audit is enabled by default. It records changes made while PgStack is running. After changing a row in your source database, connect to the PgStack server running on port `54321` from another terminal on your host:

```sh
psql 'postgres://localhost:54321/pgstack'
```

Query `audit.changes`:

```sql
-- Inspect changes to one table.
SELECT operation, before, after, committed_at
FROM audit.changes
WHERE schema = 'public' AND "table" = 'products'
ORDER BY committed_at DESC
LIMIT 50;

-- Filter on values captured in the changed row.
SELECT operation, after->>'name' AS name, committed_at
FROM audit.changes
WHERE after->>'name' ILIKE '%jacket%'
ORDER BY committed_at DESC;
```

### Search

Search works with tables that have primary keys. Save this as `pgstack.yaml`, changing the table and columns to match your database:

```yaml
# Search only. Set audit.enabled to true to also capture change history.
audit:
  enabled: false
search:
  enabled: true
  tables:
    - name: public.products
      indexColumns: [name, description]
      storeColumns: [id, name, description]
```

`indexColumns` provide the text to search. `storeColumns` are returned by queries and available for filtering; include primary-key columns to return them in results. Source names become Search names such as `public.products` → `search.public_products`.

Mount your configuration when starting the container:

```sh
docker run --rm --name pgstack \
  -p 54321:54321 \
  -e DATABASE_URL='postgres://postgres:postgres@host.docker.internal:5432/postgres' \
  --mount type=bind,src="$PWD/pgstack.yaml",dst=/app/pgstack.yaml,readonly \
  ghcr.io/pgstack-io/pgstack:latest
```

Existing rows are indexed at startup, and subsequent changes keep the index current. Once indexing completes, connect to the PgStack server running on port `54321` from another terminal on your host:

```sh
psql 'postgres://localhost:54321/pgstack'
```

Search with BM25 keyword ranking:

```sql
SELECT id, name FROM search.public_products
ORDER BY keyword_rank('rain jacket') LIMIT 10;
```

For semantic and hybrid search, add `-e OPENAI_API_KEY` to `docker run`. Without a key, indexing stays keyword-only and makes no embedding API calls.

```sql
-- Find related meaning.
SELECT id, name FROM search.public_products
ORDER BY semantic_rank('something for wet weather') LIMIT 10;

-- Combine semantic and keyword relevance.
SELECT id, name FROM search.public_products
ORDER BY hybrid_rank('waterproof rain jacket') LIMIT 10;
```

Rankings sort best matches first. Use stored columns in `WHERE` filters. With semantic indexing enabled, indexed text and semantic/hybrid queries are sent to OpenAI's `text-embedding-3-small`, billed to your account. Restarting rebuilds the index and can incur embedding charges again.

### Configuration

Use an optional `pgstack.yaml` file, mounted as shown in the Search example. Without it, Audit is enabled and Search is disabled. Enable either feature or both; Search requires at least one configured table when enabled. Invalid configuration is rejected before source setup. See [pgstack.example.yaml](pgstack.example.yaml).

| Variable | Required | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | Yes | Source Postgres connection URL |
| `OPENAI_API_KEY` | No | Embedding generation for semantic and hybrid search |
| `PGSTACK_PASSWORD` | No | SCRAM authentication for the SQL endpoint |

## Architecture

```text
                     Your Postgres
                           |
                           | logical replication
                           v
+----------------- PgStack container -------------------+
|                                                       |
|               CDC  -->  NATS  -->  Processor          |
|                          |                            |
|                 +--------+--------+                   |
|                 |                 |                   |
|                 v                 v                   |
|          Audit: Iceberg       Search: BM25            |
|            + Parquet        + Lance vectors           |
|                 |                 |                   |
|                 +--------+--------+                   |
|                          |                            |
|                          v                            |
|              Postgres-compatible server               |
|                 with embedded DuckDB                  |
|                                                       |
+--------------------------+----------------------------+
                           |
                           | wire protocol
                           v
                   Postgres clients
```

Audit stores Parquet with Iceberg metadata. Search uses Lance for BM25 and optional vectors. DuckDB runs inside PgStack's Postgres-compatible server to execute SQL over this data.

## OSS vs Cloud

The open-source core provides the Search replica and Audit history through SQL. [PgStack Cloud](https://pgstack.io/) adds the managed platform:

- **Web Control Panel**: database connections, projects, and product settings.
- **Team Management**: organizations, team membership, and shared projects.
- **Audit Trail UI**: change history, filters, and record inspection.
- **Fine-Grained Configuration**: table and column selection for Search and Audit.
- **Flexible Audit Retention**: configurable history for troubleshooting and compliance.
- **Managed Embeddings**: included embedding generation without a separate model API account.
- **Autoscaling**: managed capacity for Audit and Search workloads.
- **Production Security**: encryption, tenant isolation, IP allow rules, and SSH and VPN tunneling.
- **Automatic Updates**: service improvements, fixes, and infrastructure maintenance.
- **Support**: priority response and escalation options, with dedicated Slack support.

The bundled image is designed for local development and evaluation. For production workloads, PgStack Cloud provides a fully managed service.

## License

This project is distributed under [AGPL-3.0](LICENSE).

See [CONTRIBUTING.md](CONTRIBUTING.md) for development. For vulnerability reports, see [SECURITY.md](SECURITY.md).
