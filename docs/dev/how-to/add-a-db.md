## Add a DB

Database classes live in `src/diracx/db/sql/<dbname>`. Have a look at the `src/diracx/db/sql/dummy/` to see how to implement your own DB.

> [!NOTE]
> We do not want to use the `ORM` part of `SQLAlchemy` (only the `core`) for performance reasons

### Databases

The `job_db` object is an instance of the given db (e.g. `JobDB`).
A single transaction is used for the duration of the request and it is automatically committed when the function returns.
If an exception is raised the transaction is rolled back.
Connections are pooled between requests.
