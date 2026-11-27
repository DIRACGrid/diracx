# DiracX

DiracX is a lightweight, distributed data-management and workload management system, the successor of DIRAC. This glossary holds the project's canonical language. It is deliberately free of implementation detail: design decisions live in `docs/adr/`.

## Language

### Analytics

**Analytics platform** (OLAP):
The system that collects, stores, and analyses operational data from every DiracX domain (workload, data management, ...) to support decision-making, reporting, and monitoring. It is the successor of what DIRAC called "Accounting" and "Monitoring" — a distinction the platform deliberately does not keep.
_Avoid_: monitoring (only one use of it), accounting (a legacy DIRAC system it replaces)

**OLTP source**:
A database hosting the business logic of DiracX whose data feeds the analytics platform (e.g. JobDB, PilotAgentsDB, journalled counters).
_Avoid_: production DB, source DB

**Lakehouse**:
The append-only, versioned columnar store — a DuckLake lakehouse — holding the analytics data of one installation.
_Avoid_: data lake, data warehouse, database (it is neither a plain bucket nor a SQL database)

**Ingestion source**:
Anything that feeds the lakehouse: loaders reading OLTP sources, an event stream fed by external producers, or bulk historical loads.
_Avoid_: puller, producer (name the role, not the component)

**Loader**:
A scheduled task that extracts data from one OLTP source and loads it into the lakehouse together with its cursor, in one transaction.
_Avoid_: puller, tailer, aggregator

**Cursor**:
The position in an OLTP source beyond which a loader has already collected data, committed in the same transaction as the data it marks.
_Avoid_: offset (reserved for positions in the event stream)

**Event stream**:
The optional, ordered log of domain events (Kafka API) through which installations with fan-out or multi-source needs can feed the lakehouse.
_Avoid_: topic (that is one stream's partitioned log)

**Raw**:
The per-record data of the lakehouse; the source of truth, retained indefinitely.
_Avoid_: raw data (redundant), detail table

**Bucket**:
A pre-aggregated, time-binned table derived from raw records to accelerate queries. Prunable at any time: raw can always rebuild it.
_Avoid_: summary table, rollup

**Bucketing**:
The transform that derives buckets from raw records, by time granularity.
_Avoid_: aggregation (ambiguous with query-time aggregation)

**Source** (analytics):
A queryable relation of the lakehouse, published with its time column, its dimensions, and its metrics.
_Avoid_: table (queries may aggregate over several)

**Dimension**:
A column of a source that rows can be grouped or filtered by (e.g. VO, site, owner, status).
_Avoid_: attribute, tag

**Metric**:
An aggregate computed over rows of a source (e.g. number of jobs, average seconds).
_Avoid_: value, column

**Granularity**:
The time bucket of a query result: minute, hour, day, or all.
_Avoid_: resolution, interval

**Transition**:
A domain state change: one event, with the state left, the state entered, and when it happened.
_Avoid_: status change (verbose), event alone (ambiguous)

**Occupancy**:
The number of entities in each state over time, as a level; the level is the sum of every transition before a moment.
_Avoid_: history, WMSHistory (the legacy DIRAC mechanism this replaces)

**Power user**:
A member of the community granted direct access to the lakehouse, querying and visualising it per-VO or per-installation without going through the DiracX API.
_Avoid_: admin (overloaded), direct user

**VO scope**:
The rule that analytics rows are only visible to the community they belong to: users see their own data, their community's designated roles see all of it, and installation administrators see every community's.
_Avoid_: multi-tenancy, tenancy
