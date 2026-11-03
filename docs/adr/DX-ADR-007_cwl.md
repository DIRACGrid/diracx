# DX-ADR-007: CWL specification

## Metadata

- **Created By:** Ryunosuke O'Neil, Alexandre Boyer
- **Date:** 2026-07-14
- **Status:** Draft
- **Decision Maker(s):** TBD

## Abstract

Everything DiracX executes is described in CWL v1.2: a user job, a workgraph, and each transformation inside it are all the same kind of document. DiracX-specific information travels in *hints* under the `dirac:` namespace, and conformant CWL runners must ignore hints they do not understand, so every document is also plain, runnable CWL. The same file runs interactively with `cwltool` and local files, runs locally against grid data with `dirac-cwl-runner`, and runs at scale when submitted to DiracX.

Grid data and sandboxes are referenced through URI schemes (`lfn:`, `sandbox:`) and a `(File | string)` input type convention, resolved by a **resolution layer** that runs ahead of a stock CWL runner: a `File` is staged, a plain `string` is rewritten to a URL the application streams for itself. At submission the document is compiled into the DX-ADR-004 schema: each step's run body is stored content-addressed in `Processes`, or references a body already stored there, the hints become transformation columns, and the remaining skeleton is kept as the workgraph's `StrippedSpec`.

## Motivation

- **The tested artifact should be the submitted artifact.** DIRAC's workflow format is a bespoke XML tree that only DIRAC can parse or run, so nothing can be exercised without an installation. A user should be able to run the exact document that will run in production: interactively with local files, in CI against a fake replica catalogue, and on the grid, with no rewriting between the three.
- **One description for every producer.** A user job and a transformation's payload are the same kind of object (DX-ADR-002); a description format that only the Transformation System understood would immediately fork into two.

## Specification

### The layering rule

Every DiracX-specific field lives in a *hint* under the `dirac:` namespace (`$namespaces: {dirac: https://diracgrid.org/cwl#}`), never in `requirements`. CWL semantics do the rest: runners must ignore unknown hints but would reject unknown requirements, so any conformant runner can execute the document unchanged.

Because runners skip hints, DiracX owns their validation: the hint vocabulary is defined by a published, versioned JSON schema (`schema_version` in the `dirac:Workgraph` hint), which the submission API enforces and editors can check via the same schema mechanisms the CWL tooling already uses.

### Hint vocabulary

| Hint                   | Attaches to                       | Carries                                                                                                                                                                                                         |
| ---------------------- | --------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dirac:Job`            | a `CommandLineTool` or `Workflow` | marks a standalone user job; submission defaults and output-sandbox patterns                                                                                                                                    |
| `dirac:Workgraph`      | a top-level `Workflow`            | `schema_version`, VO behaviour `type`, launch parameters, output-sandbox patterns, data-management declarations with their initial state, its approving action list, hook-binding overrides                     |
| `dirac:Transformation` | a step                            | the packer and its arguments, a requirements template, its lifecycle action lists, hook-binding overrides, output-sandbox patterns, the executing identity, which step input drives the pool, its initial state |
| `dirac:Feeder`         | a workflow input, directly        | the feeder plugin and its arguments (the input metadata query)                                                                                                                                                  |

The exact field names, casing and permitted attachment points are fixed by the JSON schema, which is the authority; the table above fixes the roles.

CWL writes `hints` either as a mapping keyed by class or as a list of objects carrying `class`. Both are accepted and mean the same thing, and the mapping is canonical. A class then appears at most once, and the schema is plain `properties` rather than a `contains` over a discriminated list, which is the difference between a useful editor error and an unreadable one.

Every DiracX-specific field lives under the `dirac:` namespace, but not all of them live in a `hints` slot: CWL input parameters have none, so `dirac:Feeder` sits on the parameter itself as an extension field. Unknown fields on a parameter are ignored by a conformant runner in the same way unknown hints are, so the layering rule holds.

### Defaults from the configuration service

Most hint fields are optional: anything not written in the document is filled from the DiracX configuration service, resolved hierarchically (installation, then VO, then the workgraph's `type`, then the hints themselves, with `dirac:Transformation` overriding `dirac:Workgraph`). Documents stay short, and installation-wide policy such as default output-sandbox patterns or priorities lives in one place.

The resolution must also be able to run ahead of time, for backends whose worker nodes cannot call out (the push-style `diracx-remote` case). The mechanism is a **resolved-hints sidecar**: the dispatcher (DX-ADR-003) emits the effective values as a separate document, content-addressed and referenced by the parcel (DX-ADR-004), leaving the process byte-identical to its stored hash. The sidecar doubles as a `cwltool --overrides` file for the subset of values that affect CWL execution itself, and as the record of exactly which defaults were in force when the job ran. Baking the values into the document remains available as an explicit materialisation for debugging, but it changes the document's content and therefore its identity, so it is never what is stored.

### The resolution layer

DiracX-specific behaviour lives in resolution, not execution. A resolution layer turns a document plus its references into an ordinary CWL document and input object, which a stock conformant runner then executes. It has two phases, run in different places:

- **Document resolution** turns `run` references into the processes they name and emits the resolved-hints sidecar. It happens at dispatch and produces the parcel's self-contained payload.
- **Input resolution** turns `lfn:` and `sandbox:` references into staged files, URLs or extracted directories, injects the per-parcel values, and wires all of it onto the document's input parameters. It happens in the job wrapper on the node. For backends whose nodes cannot call out it runs ahead of time in the dispatching services instead, so the node performs no lookups and bulk data arrives through the site's staging channel (DX-ADR-003).

Local tooling performs both at once, which is what makes a document that runs on a laptop the same document that runs at scale.

The layer combines four sources of value: inputs supplied at submission, defaults from the configuration service, per-parcel values injected at scheduling (identifiers, the output prefix), and resolved references. Because a resolved URL may embed a credential, redacting them from logs is the layer's responsibility too.

### Document classes

A **user job** is any tool or workflow carrying `dirac:Job`. By default it needs nothing else: inputs are supplied at submission, and the wrapper resolves any references they carry and runs the document.

A **workgraph** is a `Workflow` carrying `dirac:Workgraph`, in which every step must carry `dirac:Transformation` and becomes a transformation row. A bare step is a submission-time error. The hint's fields are all optional, so `dirac:Transformation: {}` is a transformation with everything defaulted, but its presence is required deliberately: a transformation boundary implies parcelling, scheduling and staging, so it is marked rather than inferred, and unhinted steps stay reserved for future step kinds that can then be added without changing what existing documents mean. A step's `run` body may itself be a `Workflow`, which is how several programs execute inside one job (the fused reconstruction-plus-filter case). Inputs arriving from outside the workgraph carry `dirac:Feeder` hints; everything else is ordinary CWL.

```yaml
cwlVersion: v1.2
class: Workflow
$namespaces:
  dirac: https://diracgrid.org/cwl#

hints:
  - class: dirac:Workgraph
    schema_version: "1.0"

inputs:
  - id: input-data
    type: {type: array, items: [File, string]}
    dirac:Feeder:
      name: LHCbBookkeeping
      args:
        configName: LHCb
        configVersion: Collision26

steps:
  - id: reco
    hints:
      - class: dirac:Transformation
        packer: {name: ByGroupSizeRun, args: {group_size: 2}}
        requirements_template: {priority: 2, output_se: CERN-BUFFER}
    in: [{id: input-data, source: input-data}]
    out: [processed]
    run: {...} # a CommandLineTool or nested Workflow; stored content-addressed at submission

outputs:
  - id: processed
    type: File[]
    outputSource: reco/processed
```

A worked example, a workgraph plus a shared library step, runnable with stock `cwltool`, is kept at [lhcb-cwl-example](https://gitlab.cern.ch/roneil/lhcb-cwl-example).

### The DAG and its outputs are declared

Edges between transformations are ordinary CWL dataflow: a downstream step's `in` sources an upstream step's output, and the workgraph-level `outputs` declare what the workgraph delivers. Two consequences:

- Anything consumed by another step but *not* declared as a workgraph output is an intermediate, eligible for automatic removal once its consumers are done (in-job after downstream registration, or through a removal transformation).
- A step output that is neither consumed nor declared is an error at submission time.

Data-management transformations (replication, archival, removal) are declared in the `dirac:Workgraph` hint, referencing the files they act on: a step output, or a workgraph input, as when the input files staged to a buffer are removed once the consuming step has processed them. CWL dataflow has no notion of "copy this somewhere", so they are nodes of the graph that actually runs without being steps of the document.

Which members run during the scout is derived from the graph rather than declared on each of them: a member runs if it is a compute step or an ancestor of one, and data management that sits only downstream — replicating an output, removing an intermediate — is held and starts when the workgraph is approved. What separates upstream from downstream is `after` and the dataflow, never the files a member names: in [`sprucing.cwl`](../assets/examples/sprucing.cwl) the staging replication and the buffer removal both act on `raw-data`, and only `after: [sprucing]` tells them apart. Whether the workgraph scouts at all follows the same way: it scouts when there is something to scout and an approving list to judge it, since a scout nobody judges is not a scout but the first parcels of the run. `initial_state` is the one override, on a step and on a data-management declaration alike, and it says the third thing a boolean could not — `Paused`, waiting for an operator (DX-ADR-005). The size of the scout is the feeder's own argument, which is where DX-ADR-006 already puts it.

The distinction between compute- and data-transformations is analogous to the distinction between logical and physical files. The CWL document defines the logical flow of data through compute transformations. DiracX provides hints within the CWL document to manage the flow of physical data that has no direct impact on the computation.

### The output sandbox

A job produces more than its declared outputs: logs, configuration dumps, monitoring summaries, and other intermediate files that are never registered in the catalogue but are worth keeping. Files matching a set of glob patterns are persisted to the job's output sandbox.

The patterns are specified in the hints, at three levels: `dirac:Job` for a standalone user job, `dirac:Workgraph` as the default for every transformation in the workgraph (itself defaulted from the installation configuration), and `dirac:Transformation` to extend or override per step. A real production's list gives the flavour: `prodConf_*.json`, `summary*.xml`, `prmon*`, `*.log`.

Sandbox capture operates outside the dataflow, because routing log-type files through step outputs into the workgraph's declared outputs would compile them into catalogue deliverables. So that one document still collects its logs when run locally, an output parameter may be marked sandbox-bound with a `dirac:` marker, which submission compiles into sandbox patterns rather than a deliverable.

Stdout capture is a wrapper concern rather than a document concern: the wrapper captures the runner's combined stdout and stderr into the output sandbox, and CWL's own `stdout` field remains available for the case where a log is a genuine dataflow product. Nothing is injected into documents, since injection would change both their content hashes and their output contracts.

### The status report

A transformation's job says what became of each of its inputs through an output parameter that the document marks as the **status report**, with a `dirac:` marker like the sandbox-bound one. The file is JSON against a schema DiracX publishes, keyed by input id:

```json
{
  "id-1": {"action": "SUCCESS"},
  "id-2": {
    "action": "SPLIT",
    "SUCCESS": [{"lfn": "lfn:/lhcb/.../00001.raw", "lfn_size": 3000000000, "data": {"events": "1-5000"}}],
    "FAILURE": [{"lfn": "lfn:/lhcb/.../00001.raw", "lfn_size": 3000000000, "data": {"events": "5001-10000"}}]
  },
  "id-3": {"action": "FAILURE"},
  "id-4": {"action": "NOT_TRIED"}
}
```

`SUCCESS` means the input was processed in full, `FAILURE` that it was not, and `NOT_TRIED` that the job never reached it. `SPLIT` means part of it was processed: the portions that succeeded and the portions that did not are each listed as inputs, in the shape a feeder yields them (DX-ADR-006), so a child carries the mask that selects its portion. The core reads the report while the parcel is `Completing` and drives the input transitions and the recovery split from it (DX-ADR-005).

### Grid data and sandboxes: URI schemes, replica maps, and `(File | string)`

- An LFN is written as a URI: `location: lfn:/lhcb/...` on a CWL `File`.
- Input types use the union `(File | string)`, and the value supplied picks the access mode, which the resolution layer then executes. A `File` with an `lfn:` location is staged: replica lookup, download, the location rewritten to the local path, with CWL's native `size`/`checksum` fields validating the transfer. An `lfn:` string is rewritten to the best protocol URL (`root://`, `https://`) and the application streams it for itself, the `xrdcp`-style access DIRAC calls `input_data_policy: protocol`. The same document supports both; the values supplied decide.
- The replica map is a pluggable lookup. In production it is the real catalogue; locally it is a JSON file mapping LFN to replicas, size and checksum, so CI can run grid-shaped workflows with no grid at all.
- `dirac-cwl-runner` is a thin convenience for local tests: it performs the same resolution against the real catalogue (or a supplied map), then delegates to stock `cwltool`. With purely local paths, stock `cwltool` alone works.
- **Input sandboxes are the same mechanism under a second scheme.** A `sandbox:` reference into the content-addressed sandbox store is resolved by the same layer, and the declared CWL type fixes the semantics: on a `File` input, the fetched archive itself; on a `Directory` input, the archive fetched and extracted. Auxiliary files authored alongside the document, such as option files and snippets, are not sandboxes at all: they are ordinary `File` inputs or `InitialWorkDirRequirement` entries.

### References and the process library

A step's `run` may be a *reference* to an already-stored process instead of an inline body. This is not an extension: CWL types the field `[string, Process]`, and specifies that "if `run` is a string, it must be an absolute IRI or a relative path from the primary document", link-resolved like any other `@id`.

- **The reference form** is a content-hash HTTPS URL, written short through a `$namespaces` prefix (`run: proc:sha256-…`), with `#fragment` entry points into packed multi-process objects. Any conformant runner resolves it by ordinary link resolution.
- **One namespace, instance authority.** The namespace is project-owned and instance-neutral, a persistent redirect rather than an installation's hostname. The in-document binding exists for standalone use only: DiracX extracts the hash and resolves it against its own configured store, never against a host the document supplies, so a document cannot redirect the dispatcher. Canonicalisation reduces a reference to the bare hash, keeping the binding out of the document's identity, and submission pins any mutable name to the hash it resolved to.
- **Stored as a tree, named by a registry.** Nested references are stored as references rather than flattened, so a shared tool deduplicates independently of everything composing it. Over the content store sits a registry mapping a name and a version to a hash (DX-ADR-004), which gives library processes identity and lineage: asking which transformations run a step older than some fix becomes a lookup rather than a crawl.
- **What keeps deduplication real.** Canonicalisation fixes a single byte form for logically identical documents, and variation belongs in the skeleton or in the parameters, never in run bodies. Library processes are referenced, not copied, since drifting copies silently fragment the store.
- **Materialised at dispatch.** References never reach a worker node. The dispatcher assembles the referenced bodies into the parcel's self-contained payload, packed or bundled with relative references, so the node fetches nothing, needs no store credentials, and runs offline by construction on every backend. The payload is keyed by the process hash and the resolved-hints sidecar, with everything per-parcel in the input object, so a transformation's thousands of parcels share one materialisation. It is deterministic, in that `cwltool --pack` over the authored document reproduces what ran, and the parcel records the original hashes.
- **Direct fetch is a development convenience.** A stock runner may fetch public references itself, trusting TLS rather than an integrity check, and the library is world-readable for exactly that. User-submitted bodies stay VO-scoped, fetched only by tooling holding the user's credentials, restricted exactly as restricted input data is.

### Compilation into the schema

Submission decomposes the document (DX-ADR-004): each step's `run` body is canonicalised, hashed and stored in `Processes`, or is already a reference to a row there, with identical bodies collapsing to one row and nested references kept as references; the hints are extracted into transformation columns (`Feeder`/`FeederArgs`, `Packer`/`PackerArgs`, the single-hook bindings resolved as below, the requirements template) and into the rows of the ordered lifecycle-action lists; and the skeleton that remains, the DAG with run bodies replaced by hashes, is kept as the workgraph's `StrippedSpec`. When a parcel is created, user or transformation, the process, the requirement set and the resolved-hints sidecar are referenced by content hash and the resolved parameters are frozen alongside them, so the provenance of what ran is a chain of hashes.

## Rationale

- **Hints, because they are ignorable.** The whole portability story rests on one CWL rule: unknown hints are skipped, unknown requirements are fatal. Putting DiracX data anywhere else would make the documents DiracX-only, which is the property being escaped.
- **The union type, instead of a runner flag.** Staged-versus-streamed is a property of each value, not of the document, so it belongs in the type system. The document stays runnable everywhere, and the same tool body handles both (`typeof f === "string" ? f : f.path`).
- **A declared DAG.** The workgraph document states its edges, so tooling reads the structure instead of reconstructing it from query matching. Execution still serves each edge through a feeder over the files recorded as upstream parcels finish (DX-ADR-004, DX-ADR-006), which keeps neighbouring transformations decoupled at run time (DX-ADR-002).
- **Resolution as a named layer.** Staging, reference resolution and default injection were three separate mechanisms with three separate places to go wrong. Naming them one layer with two phases fixes where each runs, which is what lets the same document execute on a laptop, on a node with connectivity, and on a node with none.
- **A stock runner suffices.** Every document must remain executable by an unmodified conformant runner, and no document semantics may depend on an extension. DiracX's own stack stays free to extend the runner through its sanctioned hooks, the fetchers, filesystem access, path mapping and the JS engine, where that adds value, as internal optimisation and upstreamed where possible, never as a fork and never as behaviour a document can observe. The portability claim is then not merely that documents are valid CWL, but that a stock runner is always enough.

## Rejected Ideas

- **DIRAC's Step/Module workflow XML.** Bespoke, DIRAC-only, no external tooling or typed model; the thing being replaced.
- **Carrying DiracX data in `requirements`.** Would make every document unrunnable by anything except DiracX tooling.
- **A wrapper manifest around plain CWL.** A sidecar file describing feeders and packers splits the artifact in two; the halves would be edited and versioned separately and drift. The resolved-hints sidecar is not this: it is machine-generated at dispatch and frozen as a record, never hand-edited alongside the source.
- **Baking resolved defaults into the stored document.** An earlier draft of this ADR. It changes the document's content and therefore its hash, so the thing that ran could no longer be compared against the thing that was submitted.
- **A custom URI scheme for `run` references.** Would resolve only through runner-specific fetcher plugins, the Arvados `keep:` pattern; a content-hash HTTPS URL behind a `$namespaces` prefix reads the same and resolves everywhere. Rejected as something a document may require, though DiracX tooling remains free to register such fetchers as an internal convenience.
- **Injecting stdout capture into documents.** Would change their content hashes and their output contracts to solve what the wrapper can solve without touching them.

## Open Issues

- **The `dirac:` JSON schema.** The concrete field names, casing and defaults are the real deliverable of this ADR's design; the schema and its `schema_version` evolution policy need writing. A first version is published at [`docs/schemas/dirac-1.0.json`](https://github.com/DIRACGrid/diracx/blob/main/docs/schemas/dirac-1.0.json), covering `dirac:Workgraph`, `dirac:Transformation` and `dirac:Feeder`; it is written from the worked examples the [playground](../playground.md) ships rather than from a settled vocabulary, and the two are held in step by the checks in `tests/workgraph-sim`. The attachment forms are settled above. The sandbox-bound and status-report output markers, and the precedence between a standard `ResourceRequirement` and the requirements template, are not.
- **Canonicalisation.** The canonical form for hashing, meaning key order, JSON rendering, the treatment of `doc` and `label`, and `$import` resolution, needs pinning. Deduplication and the provenance chain are only as strong as these rules, which are shared with DX-ADR-004.
- **CWL feature subset.** Which CWL features are supported or restricted needs pinning; the existing production converter already relies on several, and the worked example requires `InlineJavascriptRequirement`, `SubworkflowFeatureRequirement`, `StepInputExpressionRequirement` and `MultipleInputFeatureRequirement`. Whether `ExpressionTool` steps are ever admitted, perhaps evaluated at dispatch rather than becoming transformations, can be decided in a later schema version, since bare steps are errors today.
- **The overrides subset.** Whether `cwltool`'s overrides mechanism can carry injected hints as well as requirements needs verifying. If it cannot, the sidecar stays authoritative for the wrapper and overrides cover only the execution-affecting subset.
- **Registry rules.** Who may publish a library name, whether a published name may later be repointed, and whether versions carry an ordering, are shared with DX-ADR-004.
