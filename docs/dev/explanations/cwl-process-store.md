# CWL process store

When a workgraph is submitted, the process each transformation runs is stored
in the `Processes` table of the `TransformationDB`
([DX-ADR-004](../../adr/DX-ADR-004_schema.md)), keyed by its identity. How
the identity is computed is explained in
[CWL process identity](cwl-process-identity.md); this page covers what is
checked before a process is stored, what a row holds, and what a parcel reads
back.

## One row per transformation

A transformation's process is usually a small `Workflow` around one or more
`CommandLineTool`s, possibly with nested workflows. It is stored **flat**: one
row holds `subgraph(document, entry_id)`, the transformation's entry and every
entry it runs, exactly as submitted. Loading what a parcel runs is a single
primary-key read, and the row is a packed document a stock CWL runner
executes.

For the MCSimulation workgraph that is three rows:

| Transformation   | Row key         | Row holds                                               |
| ---------------- | --------------- | ------------------------------------------------------- |
| MCSimulation     | `e1e266130b5d…` | `#gauss.cwl`, `#lb-run-app.cwl`                         |
| MCReconstruction | `31cd3f1f36f1…` | `#boole-moore.cwl`, `#digi-reco.cwl`, `#lb-run-app.cwl` |
| MCMerge          | `9d84d8c7268c…` | `#merge.cwl`, `#lb-run-app.cwl`                         |

The shared tool is copied into each row. The alternative described in the
ADR, one row per process with `ProcessReferences` edges, was measured on a
year of LHCb submissions and saved nothing: almost all variation between jobs
is inside the tools, so the tree deduplicates little, while costing more rows
and a recursive query at dispatch.

## Storing a process

`diracx.logic.transformation.processes.store_process(db, document, entry_id)`
takes the packed document the client submitted and one of its entries:

1. **Self-contained** (`check_self_contained`): nothing in the document may
    need fetching, whether by the services or by a runner.
2. **Policy** (`check_policy`): only requirements and hints every backend can
    honour, no image built or fetched on the worker node, no abstract
    `Operation`, at most 256 kB per entry.
3. **Hash first** (`entry_identity`): a process already in the store was
    validated when it was stored, so resubmitting it costs one lookup.
4. **Validate** (`validate_packed`): cwl-utils loads every entry of the row
    against the CWL v1.2 schema, with a fetcher that refuses to read anything
    but the row itself. How step inputs are wired is not checked yet:
    cwl-utils' static checker will do that once a release includes
    common-workflow-language/cwl-utils#480.
5. **Store** (`insert_processes`): an insert that skips existing keys, so
    concurrent submissions of the same process are safe.

## Loading a process

`load_process(db, process_id)` returns the row and the entry to start from,
for example `#merge.cwl`, which a runner is given as `row.json#merge.cwl`.
Rows keep the ids they were submitted with, so the entry is found as the one
whose identity is the row's key. This also checks that the row still matches
its key: a database may re-sort keys and reformat numbers in a `JSON` column,
but a row read back must hash the same.

## Not yet covered

- lineage, such as which transformations run a given tool;
- `ProcessRegistry` names and versions, garbage collection, and VO visibility
    of stored processes;
- pinning mutable references, such as container tags: a `dockerPull` tag is
    accepted as written, so one identity can name different software if the
    tag moves;
- checking how step inputs are wired, until cwl-utils' static checker is
    fixed.
