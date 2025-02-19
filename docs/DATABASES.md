# Databases

The primary store of operational data in DiracX is in SQL databases managed through SQLAlchemy.
In addition, DiracX utilizes OpenSearch (or Elasticsearch) for storing pilot logs, medium-term metadata about jobs and pilots ("Job Parameters" and "Pilot Parameters"), and optionally, for OpenTelemetry data.
Access to databases is managed by the `diracx-db` package.

## SQL Databases

Access to the SQL databases in DiracX is always handled through SQLAlchemy's core functionality.
While the code is designed to be compatible with multiple database systems, only MySQL and MariaDB are officially supported in production deployments.
Support for Postgresql or other databases will be added if demand and resources are found.
For convenience SQLite is used for testing and development however this should never be used in production.

### Connecting to SQL databases

Connections to DiracX DBs are configured using the [SQLAlchemy connection URL format](https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls).
DiracX requires that the `driver` part of the URL is always specified and it must refer to an async-compatible backend.
The value of this URL is taken from the environment vairable of the form `DIRACX_DB_URL_<db-name>`, where `<db-name>` is defined by the entry in the `diracx.dbs.sql` entrypoint in the `pyproject.toml`.

```bash
export DIRACX_DB_URL_MYDB="mysql+aiomysql://user:pass@hostname:3306/MyDB"
```

See the DiracX helm chart for more details about configuring access to databases.


### Using SQL databases

See the services/tasks documentation for details about how the database classes should be used.

### API

TODO: Document the basic API for SQL databases once things settle

## OpenSearch Databases

DiracX also utilizes OpenSearch databases for storing pilot logs, medium-term metadata about jobs and pilots, and optionally, OpenTelemetry data.

### Connecting to OpenSearch databases

Connections to OpenSearch and ElasticSearch DBs are configured through environment variables prefixed with `DIRACX_OS_DB_{DB_NAME}`.
The connection parameters are defined as a JSON mapping and loaded from the environment variables.
See the [upstream documentation](https://opensearch.org/docs/latest/clients/python-low-level/#connecting-to-opensearch) for details about the available options.

```bash
DIRACX_OS_DB_MYDB='{"hosts": "user:pass@hostname:9200", "use_ssl": true, "verify_certs": true}'
```

### Using OpenSearch databases

To interact with OpenSearch databases, DiracX provides the `BaseOSDB` class, which is designed to handle the lifecycle of the OpenSearch client and perform various database operations such as creating index templates, inserting documents, and querying data.

### API

TODO: Document the basic API for opensearch databases once things settle
