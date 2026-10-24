# CWL process store

Everything DiracX executes is described in CWL ([DX-ADR-007](../../adr/DX-ADR-007_cwl.md)).
When a workgraph is submitted, the process each transformation runs is stored
in the `Processes` table of the `TransformationDB`
([DX-ADR-004](../../adr/DX-ADR-004_schema.md)), keyed by a hash of its content.
This page explains how that identity is computed, how a process gets into the
store, and what a parcel reads back.

## One row per transformation

A transformation's process is usually a small `Workflow` around one or more
`CommandLineTool`s, possibly with nested sub-workflows. It is stored **flat**:
one `Processes` row holds the whole subgraph as a packed CWL document
(`$graph`), so materialising the payload a parcel runs is a single
primary-key read, and the row is exactly the document a stock CWL runner
executes.

The alternative described in the ADR, one row per CWL process with
`ProcessReferences` edges, was measured on a year of LHCb submissions and
gave no saving: almost all variation between jobs is inside the leaf tools,
so the tree deduplicates little, while costing more rows and a recursive query
at dispatch. The benchmark is in `benchmarks/process_store/`.

## Identity

A process is identified by the SHA-256 of its canonical form,
`diracx.core.cwl.canonical`, which is defined over the parsed document rather
than over any CWL library's output, so that it is deterministic, stable across
library upgrades and reproducible by any tool. In short (the module docstring
has the full rules, `CANONICAL_VERSION` numbers them):

- process `id`s, `$namespaces`, `dirac:` fields and hints, and `doc` and
    `label` where they are metadata are dropped: the identity is what runs,
    not what it is called, where it was loaded from, or how DiracX schedules
    it. Prefixes are first expanded to full URIs, so the identity covers what
    a prefix means, and names a user chose (an environment variable, a record
    field) are kept even when they are called `doc` or `label`;
- lists keyed by `id` or `class` are written as maps, and fully scoped
    identifiers are shortened against their scope, so formatting and absolute
    `file://` URIs do not matter;
- named types are written by short name, and a process may only use the
    named types it defines itself, so its identity covers their definitions;
- each nested `run` body is replaced by its own identity, so a workflow's
    identity covers its children's (a Merkle tree);
- the result is JSON with sorted keys and no whitespace.

The identity of a transformation is the root of that tree. The packed
document names every component by its own identity (`#sha256-<hex>`), so the
tree can be recovered from a stored row (`unpack`), and a component index or
per-component rows can be added later without changing any identity.

The hash is over the value, not the stored bytes: MySQL's `JSON` type
re-sorts keys and reformats numbers, so a row read back is re-canonicalised to
check it, which `diracx-db/tests/transformation/test_round_trip.py` verifies
on MySQL and MariaDB in CI.

## From files to a stored process

```mermaid
flowchart LR
    files[".cwl files (YAML)"] -->|"resolved by the CLI"| doc["one JSON value"]
    doc -->|decompose| tree["canonical tree"]
    tree -->|check_policy| tree
    tree -->|"already stored?"| done["identity"]
    tree -->|pack| packed["packed document"]
    packed -->|"validate_packed (cwl-utils)"| store["Processes row"]
    store --> done
```

1. **Resolve** (in the CLI): the YAML files are read and every step's `run`
    path, `$import`, `$include` and `#fragment` is inlined, so that one JSON
    value is submitted. References to stored processes (`proc:sha256-<hex>`)
    are kept. YAML stays in the CLI: services only receive JSON. They refuse
    any `$import`, `$include` or other directive left in it, and validation
    never reads anything but the packed document.
2. **Canonicalise** (`decompose`): compute the tree and its identities.
    Only CWL v1.2 is accepted; older documents are refused with a request to
    run `cwl-upgrader`, since upgrading on the server would change what the
    user tested.
3. **Policy** (`check_policy`): requirements outside an allow-list that every
    backend can honour are refused, `dirac:` requirements included. So are
    abstract `Operation`s, which no runner can execute, and processes larger
    than 256 kB, whose content belongs in a sandbox or `File` input. Hints are
    not restricted. `DockerRequirement` is accepted, to run with Apptainer,
    when it names an image to pull (`dockerPull`, `dockerImageId`); its
    `dockerFile`, `dockerLoad` and `dockerImport` fields are refused, even in
    a hint, since they build or fetch an image on the worker node.
4. **Hash first**: a process already in the store was validated when it was
    stored, so resubmitting it costs a lookup.
5. **Validate** (`diracx.logic.transformation.validation.validate_packed`):
    cwl-utils' parser loads every component of the packed document, checking
    it against the CWL v1.2 schema. Validating the packed document validates
    exactly what parcels will run. How step inputs are wired is not checked
    yet: cwl-utils' static checker will do that once a release includes
    common-workflow-language/cwl-utils#480.
6. **Store**: `INSERT IGNORE`, so concurrent submissions of the same process
    are safe.

A `run` may reference a stored process by identity, provided it is the root
of a stored row; its body is inlined into the new row.

## Not yet covered

- per-component rows, lineage ("which transformations contain this tool") and
    references to arbitrary components;
- `ProcessRegistry` names and versions, garbage collection, and VO visibility
    of stored bodies;
- pinning mutable references inside bodies, such as container tags: a
    `dockerPull` tag is accepted as written, so the same identity can name
    different software if the tag moves;
- recording `CANONICAL_VERSION` per row;
- checking how step inputs are wired, until cwl-utils' static checker is
    fixed.
