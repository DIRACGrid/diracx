# Databases

The primary store of operational data in DiracX is in SQL databases managed through SQLAlchemy.
In addition, DiracX utilizes OpenSearch (or Elasticsearch) for storing pilot logs, medium-term metadata about jobs and pilots ("Job Parameters" and "Pilot Parameters"), and optionally, for OpenTelemetry data.
Access to databases is managed by the `diracx.dbs` package.

## SQL Databases

Access to the SQL databases in DiracX is always handled through SQLAlchemy's core functionality.
While the code is designed to be compatible with multiple database systems, only MySQL and MariaDB are officially supported in production deployments.
For convince SQLite is used for testing and development however this should never be used in production.

### Connecting to SQL databases

Connections to DiracX DBs are configured using the [SQLAlchemy connection URL format](https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls).
DiracX requires that the `driver` part of the URL is always specified and it must refer to an async-compatible backend.
The value of this URL is taken from the environment vairable of the form `DIRACX_DB_URL_<db-name>`, where `<db-name>` is defined by the entry in the `diracx.db.sql` entrypoint in the `pyproject.toml`.
See the DiracX helm chart for more details about configuring access to databases.

### Using SQL databases

See the services/tasks documentation for details about how the database classes should be used.

### API

TODO: Document the basic API for SQL databases once things settle

### Implementation details

A SQL database in DiracX is defined as a subclass of `diracx.dbs.sql.utils.BaseSQLDB`.
The details covered here should be handled automatically by the service and task machinery of DiracX and this documentation exists for informational purposes.

#### Discovery

The available databases are discovered by calling `BaseSQLDB.available_urls`.
This method returns a mapping of database names to connection URLs.
The available databases are determined by the `diracx.dbs.sql` entrypoint in the `pyproject.toml` file and the connection URLs are taken from the environment vairables of the form `DIRACX_DB_URL_<db-name>`.

If extensions to DiracX are being used, there can be multiple implementations of the same database.
To list the available implementations use `BaseSQLDB.available_implementations(db_name)`.
The first entry in this list will be the preferred implementation and it can be initialized by calling it's `__init__` function with a URL perviously obtained from `BaseSQLDB.available_urls`.

#### Engine and transaction management

To control the lifetime of the SQLAlchemy engine used for connecting to the database, which includes the connection pool, the `BaseSQLDB.engine_context` asynchronous context manager should be entered.
When inside this context manager, the engine can be accessed with `BaseSQLDB.engine`.

Upon entering, the DB class can then be used as an asynchronous context manager to enter transactions.
If an exception is raised the transaction is rolled back automatically.
If the inner context exits peacefully, the transaction is committed automatically.
When inside this context manager, the DB connection can be accessed with `BaseSQLDB.conn`.

For example:

```python
db_name = ...
url = BaseSQLDB.available_urls()[db_name]
MyDBClass = BaseSQLDB.available_implementations(db_name)[0]

db = MyDBClass(url)
async with db.engine_context:
    async with db:
        # Do something in the first transaction
        # Commit will be called automatically when exiting the "async with db:" context manager

    async with db:
        # This transaction will be rolled back due to the exception
        raise Exception(...)
```

## OpenSearch Databases

DiracX also utilizes OpenSearch databases for storing pilot logs, medium-term metadata about jobs and pilots, and optionally, OpenTelemetry data.

### Connecting to OpenSearch databases

Connections to OpenSearch and ElasticSearch DBs are configured through environment variables prefixed with `DIRACX_OS_DB_{DB_NAME}`.
The connection parameters are defined as a JSON mapping and loaded from the environment variables.
See the [upstream documentation](https://opensearch.org/docs/latest/clients/python-low-level/#connecting-to-opensearch) for details about the available options.

### Using OpenSearch databases

To interact with OpenSearch databases, DiracX provides the `BaseOSDB` class, which is designed to handle the lifecycle of the OpenSearch client and perform various database operations such as creating index templates, inserting documents, and querying data.

### API

TODO: Document the basic API for opensearch databases once things settle

### Implementation details

An OpenSearch database in DiracX is defined as a subclass of `BaseOSDB`. The details covered here should be handled automatically by the service and task machinery of DiracX and this documentation exists for informational purposes.

#### Discovery

The available OpenSearch databases are discovered by calling `BaseOSDB.available_urls`.
This method returns a dictionary of database names to connection parameters.
The available databases are determined by the `diracx.db.os` entrypoint in the `pyproject.toml` file and the connection parameters are taken from the environment variables prefixed with `DIRACX_OS_DB_{DB_NAME}`.

If extensions to DiracX are being used, there can be multiple implementations of the same database.
To list the available implementations use `BaseOSDB.available_implementations(db_name)`.
The first entry in this list will be the preferred implementation and it can be initialized by calling its `__init__` function with the connection parameters previously obtained from `BaseOSDB.available_urls`.

#### Client and transaction management

To control the lifetime of the OpenSearch client, the `BaseOSDB.client_context` asynchronous context manager should be entered.
When inside this context manager, the client can be accessed with `BaseOSDB.client`.

Upon entering, the DB class can then be used as an asynchronous context manager to perform operations.
Currently this context manager has no effect however it must be used as it may be used in future.
When inside this context manager, the DB connection can be accessed with `BaseOSDB.client`.

For example:

```python
db_name = ...
conn_params = BaseOSDB.available_urls()[db_name]
MyDBClass = BaseOSDB.available_implementations(db_name)[0]

db = MyDBClass(conn_params)
async with db.client_context:
    async with db:
        # Do something with the OpenSearch client
```
