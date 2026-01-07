# BemiDB

BemiDB is an open-source Snowflake and Fivetran alternative bundled together. It seamlessly connects to different data sources, syncs data in a compressed columnar format to S3, and allows you to run complex queries using its Postgres-compatible analytical query engine.

![BemiDB](/img/BemiDB.gif)

## Contents

- [Highlights](#highlights)
- [Use cases](#use-cases)
- [Quickstart](#quickstart)
- [Usage](#usage)
  - [Syncing from Amplitude](#syncing-from-amplitude)
  - [Syncing from Attio](#syncing-from-attio)
  - [Syncing from Dialpad](#syncing-from-dialpad)
  - [Syncing from Postgres](#syncing-from-postgres)
  - [Customizing S3 endpoint](#customizing-s3-endpoint)
- [Configuration](#configuration)
- [Architecture](#architecture)
- [Benchmark](#benchmark)
- [Data type mapping](#data-type-mapping)
- [Roadmap](#roadmap)
- [License](#license)

## Highlights

- **Query Engine**: leverages a analytical query engine that is 2000x faster than regular Postgres.
- **Scalable Storage**: stores data in columnar format in object storage separated from compute.
- **Built-In Connectors**: automatically syncs data from different data sources.
- **Compressed Data**: uses an open table format with 4x data compression.
- **Easy Deployment**: packaged in a single Docker image with stateless processes.
- **Postgres Compatibility**: integrates with services and tools in the Postgres ecosystem.
- **Open-Source**: released under an OSI-approved license.

## Use cases

- **Centralize data without complex pipelines**. No complex setup and no weird acronyms like CDC or ETL.
- **Integrate with Postgres tools and services**. Querying data with BI tools, notebooks, and ORMs.
- **Run complex analytical queries at high speed**. Without worrying about performance impact or indexing.
- **Continuously archive data from your database**. Offloading and querying historical data.

## Quickstart

#### 1. Configure prerequisites for BemiDB:

  - Set up a Postgres database as a data catalog for files stored in object storage:

```sql
CREATE USER catalog_user LOGIN PASSWORD 'password';
CREATE DATABASE catalog OWNER catalog_user;
```

  - Create an S3 bucket and IAM user credentials with access to the bucket.

<a name="iam"></a>
<details>
<summary>See AWS IAM policy example</summary>

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::[AWS_S3_BUCKET]",
                "arn:aws:s3:::[AWS_S3_BUCKET]/*"
            ]
        }
    ]
}
```
</details>

  - Export the environment variables:

```sh
# Configured catalog database URL (host.docker.internal allows connecting to localhost from a container)
export CATALOG_DATABASE_URL=postgres://catalog_user:password@host.docker.internal:5432/catalog

# AWS S3 environment variables (data will be stored in s3://bemidb-bucket/iceberg/*)
export AWS_REGION=us-west-1
export AWS_S3_BUCKET=bemidb-bucket
export AWS_ACCESS_KEY_ID=[...]
export AWS_SECRET_ACCESS_KEY=[...]
```

#### 2. Sync data from a source Postgres database:

```sh
docker run \
  -e SOURCE_POSTGRES_DATABASE_URL=postgres://user:password@host.docker.internal:5432/source \
  -e DESTINATION_SCHEMA_NAME=postgres \
  -e AWS_REGION -e AWS_S3_BUCKET -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e CATALOG_DATABASE_URL \
  ghcr.io/bemihq/bemidb:latest syncer-postgres
```

#### 3. Start the BemiDB database server:

```sh
docker run \
  -p 54321:54321 \
  -e AWS_REGION -e AWS_S3_BUCKET -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e CATALOG_DATABASE_URL \
  ghcr.io/bemihq/bemidb:latest server
```

#### 4. Query BemiDB with with a Postgres client:

```sh
# List all tables
psql postgres://localhost:54321/bemidb -c "SELECT table_schema, table_name FROM information_schema.tables"

# Query a table
psql postgres://localhost:54321/bemidb -c "SELECT COUNT(*) FROM postgres.[table_name]"
```

## Usage

#### Syncing from Amplitude

1. Create an [Amplitude API key](https://docs.gettelio.com/integrations/amplitude)
2. Run the syncer:

```sh
docker run \
  -e SOURCE_AMPLITUDE_API_KEY=[...] \
  -e SOURCE_AMPLITUDE_SECRET_KEY=[...] \
  -e SOURCE_AMPLITUDE_START_DATE=2025-01-01 \
  -e DESTINATION_SCHEMA_NAME=amplitude \
  -e AWS_REGION -e AWS_S3_BUCKET -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e CATALOG_DATABASE_URL \
  ghcr.io/bemihq/bemidb:latest syncer-amplitude
```

#### Syncing from Attio

1. Create an [Attio API access token](https://docs.gettelio.com/integrations/attio)
2. Run the syncer:

```sh
docker run \
  -e SOURCE_ATTIO_API_ACCESS_TOKEN=[...] \
  -e DESTINATION_SCHEMA_NAME=attio \
  -e AWS_REGION -e AWS_S3_BUCKET -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e CATALOG_DATABASE_URL \
  ghcr.io/bemihq/bemidb:latest syncer-attio
```

#### Syncing from Dialpad

1. Create a [Dialpad API key](https://docs.gettelio.com/integrations/dialpad)
2. Create a webhook endpoint:

```sh
curl -X POST "https://dialpad.com/api/v2/webhooks" \
     -H "Content-Type: application/json" \
     -H "Authorization: Bearer [DIALPAD_API_KEY]" \
     -d '{
           "hook_url": "https://[YOUR_DOMAIN]/[YOUR_WEBHOOK_ENDPOINT]",
           "secret": "[YOUR_WEBHOOK_SECRET]"
         }'
```

3. Subscribe to SMS events for the created webhook:

```sh
curl -X POST "https://dialpad.com/api/v2/subscriptions/sms" \
     -H "Content-Type: application/json" \
     -H "Authorization: Bearer [DIALPAD_API_KEY]" \
     -d '{
           "direction": "all",
           "enabled": true,
           "endpoint_id": "[WEBHOOK_ID]",
           "include_internal": false,
           "status": false
         }'
```

4. Write a small service to receive Dialpad webhook events and publish them to NATS JetStream.

<details>
<summary>See example code in Node.js</summary>

```ts
import express from 'express';
import bodyParser from 'body-parser';
import { jwtVerify } from 'jose';
import { connect, JSONCodec } from 'nats';

const app = express();
app.use(bodyParser.json());
app.post('/dialpad-webhook', async (req, res) => {
  const { payload } = await jwtVerify(request.body, new TextEncoder().encode('[YOUR_WEBHOOK_SECRET]'), { algorithms: ['HS256'] });
  const jsonCodec = JSONCodec();
  const natsConnection = await connect({ servers: "nats://host.docker.internal:4222" });
  const jetstreamManager = await natsConnection.jetstreamManager();
  await jetstreamManager.streams.add({ name: 'bemidb', subjects: ['bemidb.dialpad'] });
  await jetstreamManager.jetstream().publish('bemidb.dialpad', jsonCodec.encode(payload));
});
app.listen(3000, () => console.log('Server is running on port 3000'));
```
</details>

5. Run the syncer:

```sh
docker run \
  -e NATS_URL=nats://host.docker.internal:4222 \
  -e NATS_JETSTREAM_STREAM=bemidb \
  -e NATS_JETSTREAM_SUBJECT=bemidb.dialpad \
  -e NATS_JETSTREAM_CONSUMER_NAME=bemidb-dialpad \
  -e DESTINATION_SCHEMA_NAME=dialpad \
  -e AWS_REGION -e AWS_S3_BUCKET -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e CATALOG_DATABASE_URL \
  ghcr.io/bemihq/bemidb:latest syncer-dialpad
```

#### Syncing from Postgres

By default, BemiDB syncs all tables from the Postgres database. To include and sync only specific tables from your Postgres database:

```sh
docker run \
  -e SOURCE_POSTGRES_DATABASE_URL=postgres://user:password@host.docker.internal:5432/source \
  -e SOURCE_POSTGRES_INCLUDE_TABLES=public.table1,public.table2 \ # A comma-separated list of tables to include
  -e DESTINATION_SCHEMA_NAME=postgres \
  -e AWS_REGION -e AWS_S3_BUCKET -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e CATALOG_DATABASE_URL \
  ghcr.io/bemihq/bemidb:latest syncer-postgres
```

To exclude specific tables during the sync:

```sh
docker run \
  -e SOURCE_POSTGRES_DATABASE_URL=postgres://user:password@host.docker.internal:5432/source \
  -e SOURCE_POSTGRES_EXCLUDE_TABLES=public.audit_log,public.cache \ # A comma-separated list of tables to exclude
  -e DESTINATION_SCHEMA_NAME=postgres \
  -e AWS_REGION -e AWS_S3_BUCKET -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e CATALOG_DATABASE_URL \
  ghcr.io/bemihq/bemidb:latest syncer-postgres
```

#### Customizing S3 endpoint

BemiDB can work with various S3-compatible object storage solutions, such as MinIO.

You can run MinIO locally:

```sh
minio server ./minio-data
# API: http://172.17.0.3:9000  http://127.0.0.1:9000
# WebUI: http://172.17.0.3:9001 http://127.0.0.1:9001
```

Create a bucket named `bemidb-bucket` in MinIO:

```sh
mc alias set local http://localhost:9000 minioadmin minioadmin123
mc mb local/bemidb-bucket --ignore-existing
```

Export and use environment variables when starting BemiDB:

```sh
export AWS_REGION=us-west-1
export AWS_S3_BUCKET=bemidb-bucket
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin123
export AWS_S3_ENDPOINT=http://localhost:9000
```

## Configuration

#### `syncer-amplitude` command options

| Environment variable          | Default value | Description                                                        |
|-------------------------------|---------------|--------------------------------------------------------------------|
| `DESTINATION_SCHEMA_NAME`     | Required      | Schema name in BemiDB to sync data to.                             |
| `SOURCE_AMPLITUDE_API_KEY`    | Required      | Amplitude API key for authentication.                              |
| `SOURCE_AMPLITUDE_SECRET_KEY` | Required      | Amplitude secret key for authentication.                           |
| `SOURCE_AMPLITUDE_START_DATE` | `2025-01-01`  | Start date for syncing data from Amplitude in `YYYY-MM-DD` format. |

#### `syncer-attio` command options

| Environment variable            | Default value | Description                                |
|---------------------------------|---------------|--------------------------------------------|
| `DESTINATION_SCHEMA_NAME`       | Required      | Schema name in BemiDB to sync data to.     |
| `SOURCE_ATTIO_API_ACCESS_TOKEN` | Required      | Attio API access token for authentication. |

#### `syncer-dialpad` command options

| Environment variable           | Default value | Description                                                    |
|--------------------------------|---------------|----------------------------------------------------------------|
| `DESTINATION_SCHEMA_NAME`      | Required      | Schema name in BemiDB to sync data to.                         |
| `NATS_URL`                     | Required      | NATS server URL for connecting to receive Dialpad SMS records. |
| `NATS_JETSTREAM_STREAM`        | Required      | NATS JetStream stream name.                                    |
| `NATS_JETSTREAM_SUBJECT`       | Required      | NATS JetStream subject name.                                   |
| `NATS_JETSTREAM_CONSUMER_NAME` | Required      | NATS JetStream consumer name.                                  |
| `NATS_FETCH_TIMEOUT_SECONDS`   | `30`          | Timeout in seconds for fetching messages from NATS.            |

#### `syncer-postgres` command options

| Environment variable                | Default value | Description                                                          |
|-------------------------------------|---------------|----------------------------------------------------------------------|
| `DESTINATION_SCHEMA_NAME`           | Required      | Schema name in BemiDB to sync data to.                               |
| `SOURCE_POSTGRES_DATABASE_URL`      | Required      | Postgres database URL to sync data from.                             |
| `SOURCE_POSTGRES_INCLUDE_TABLES`    |               | List of tables to include in sync. Comma-separated `schema.table`.   |
| `SOURCE_POSTGRES_EXCLUDE_TABLES`    |               | List of tables to exclude from sync. Comma-separated `schema.table`. |

#### `server` command options

| Environment variable | Default value | Description                            |
|----------------------|---------------|----------------------------------------|
| `BEMIDB_HOST`        | `0.0.0.0`     | Host for BemiDB to listen on           |
| `BEMIDB_PORT`        | `54321`       | Port for BemiDB to listen on           |
| `BEMIDB_DATABASE`    | `bemidb`      | Database name                          |
| `BEMIDB_USER`        |               | Database user. Allows any if empty     |
| `BEMIDB_PASSWORD`    |               | Database password. Allows any if empty |

#### Common options

| Environment variable                 | Default value      | Description                                          |
|--------------------------------------|--------------------|------------------------------------------------------|
| `CATALOG_DATABASE_URL`               | Required           | Postgres database URL for the catalog                |
| `AWS_REGION`                         | Required           | AWS region                                           |
| `AWS_S3_BUCKET`                      | Required           | AWS S3 bucket name                                   |
| `AWS_ACCESS_KEY_ID`                  | Required           | AWS access key ID                                    |
| `AWS_SECRET_ACCESS_KEY`              | Required           | AWS secret access key                                |
| `AWS_S3_ENDPOINT`                    | `s3.amazonaws.com` | Custom S3 endpoint URL                               |
| `BEMIDB_LOG_LEVEL`                   | `INFO`             | Log level: `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE` |
| `BEMIDB_DISABLE_ANONYMOUS_ANALYTICS` | `false`            | Disable collection of anonymous usage metadata       |

## Architecture

BemiDB consists of the following main components packaged in a single Docker image:

- **Database Server**: implements the [Postgres protocol](https://www.postgresql.org/docs/current/protocol.html) to enable Postgres compatibility.
- **Query Engine**: embeds the [DuckDB](https://duckdb.org/) query engine to run analytical queries.
- **Storage Layer**: uses the [Iceberg](https://iceberg.apache.org/) open table format to store data in columnar compressed Parquet files.
- **Data Syncers**: to connect to different data sources and sync data to the storage layer.

<img src="/img/architecture.png" alt="Architecture" width="720px">

## Benchmark

BemiDB is optimized for analytical workloads and can run complex queries up to 2000x faster than regular Postgres.

On the TPC-H benchmark with 22 sequential queries, BemiDB outperforms Postgres by a significant margin:

* Scale factor: 0.1
  * BemiDB unindexed: 2.3s 👍
  * Postgres unindexed: 1h23m13s 👎 (2,170x slower)
  * Postgres indexed: 1.5s 👍 (99.97% bottleneck reduction)
* Scale factor: 1.0
  * BemiDB unindexed: 25.6s 👍
  * Postgres unindexed: ∞ 👎 (infinitely slower)
  * Postgres indexed: 1h34m40s 👎 (220x slower)

See the [benchmark](/benchmark) directory for more details.

## Data type mapping

Primitive data types are mapped as follows:

| PostgreSQL                                                  | Parquet                                                         | Iceberg                          |
|-------------------------------------------------------------|-----------------------------------------------------------------|----------------------------------|
| `bool`                                                      | `BOOLEAN`                                                       | `boolean`                        |
| `bit`, `int2`, `int4`                                       | `INT32`                                                         | `int`                            |
| `int8`,                                                     | `FIXED_LEN_BYTE_ARRAY(9)` (`DECIMAL(38, 0)` / `DECIMAL(38, 0)`) | `decimal(38, 0)`                 |
| `xid`                                                       | `INT64`                                                         | `long`                           |
| `xid8`, `interval`                                          | `FIXED_LEN_BYTE_ARRAY(9)` (`DECIMAL(38, 6)` / `DECIMAL(38, 6)`) | `decimal(38, 6)`                 |
| `float4`                                                    | `FLOAT`                                                         | `float`                          |
| `float8`                                                    | `DOUBLE`                                                        | `double`                         |
| `numeric`                                                   | `FIXED_LEN_BYTE_ARRAY(16)` (`DECIMAL(P, S)` / `DECIMAL(P, S)`)  | `decimal(P, S)`                  |
| `date`                                                      | `INT32` (`DATE` / `DATE`)                                       | `date`                           |
| `time`, `timetz`                                            | `INT64` (`TIME_MICROS`)                                         | `time`                           |
| `timestamp`, `timestamptz`                                  | `INT64` (`TIMESTAMP_MICROS`)                                    | `timestamp`                      |
| `varchar`, `text`, `bpchar`                                 | `BYTE_ARRAY` (`STRING` / `UTF8`)                                | `string`                         |
| `uuid`                                                      | `BYTE_ARRAY` (`STRING` / `UTF8`)                                | `string`                         |
| `bytea`                                                     | `BYTE_ARRAY`                                                    | `binary`                         |
| `point`, `line`, `lseg`, `box`, `path`, `polygon`, `circle` | `BYTE_ARRAY` (`STRING` / `UTF8`)                                | `string`                         |
| `cidr`, `inet`, `macaddr`, `macaddr8`                       | `BYTE_ARRAY` (`STRING` / `UTF8`)                                | `string`                         |
| `tsvector`, `xml`, `pg_snapshot`                            | `BYTE_ARRAY` (`STRING` / `UTF8`)                                | `string`                         |
| `json`, `jsonb`                                             | `BYTE_ARRAY` (`STRING` / `UTF8`)                                | `string` (JSON logical type)     |
| `_*` (array)                                                | `LIST` `*`                                                      | `list`                           |
| `*` (user-defined type)                                     | `BYTE_ARRAY` (`STRING` / `UTF8`)                                | `string`                         |

Note that Postgres `json` and `jsonb` types are implemented as JSON logical types and stored as strings (Parquet and Iceberg don't support unstructured data types).
You can query JSON columns using standard operators, for example:

```sql
SELECT * FROM [TABLE] WHERE [JSON_COLUMN]->>'[JSON_KEY]' = '[JSON_VALUE]';
```

## Roadmap

- [x] Postgres protocol and query support
- [x] Iceberg write operations
- [x] Selective data syncing from Postgres
- [x] Postgres compatibility with other tools
  - [x] psql
  - [x] Metabase
  - [x] TablePlus
  - [x] DBeaver
  - [x] pgAdmin
  - [x] Grafana
  - [x] Retool
  - [ ] Jupyter notebooks ([#27](https://github.com/BemiHQ/BemiDB/issues/27))
- [x] Data syncing from other sources
  - [x] Amplitude (incremental)
  - [x] Attio CRM (full-refresh)
  - [x] Postgres (full-refresh)
  - [x] Dialpad (real-time)
  - [ ] HubSpot
  - [ ] Stripe
  - [ ] Google Sheets
  - [ ] MySQL
  - [ ] SQLite ([#24](https://github.com/BemiHQ/BemiDB/issues/24))
- [x] Iceberg tables compaction
- [x] Packaging in a Docker image
- [x] Table compaction without Trino as a dependency
- [x] Materialized views
- [x] Transformations with dbt ([#25](https://github.com/BemiHQ/BemiDB/issues/25))
- [ ] Partitioned tables ([#15](https://github.com/BemiHQ/BemiDB/issues/15))

Are you looking for real-time data syncing? Check out [BemiDB Cloud](https://bemidb.com), our managed data platform.

## License

Distributed under the terms of the [AGPL-3.0 License](/LICENSE). If you need to modify and distribute the code, please release it to contribute back to the open-source community.
