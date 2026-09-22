# DX-ADR-003: Compute backends

## Metadata

- **Created By:** Chris Burr, Christophe Haen
- **Date:** 2026-07-10
- **Status:** Draft
- **Decision Maker(s):** TBD

## Abstract

Compute **parcels**, whether created by a transformation's packer or submitted as user jobs, are executed by pluggable **compute backends**. This ADR defines the backend contract (`submit`, `poll`/`retrieve`, `cancel`), the points at which it drives the parcel state machine, and the per-backend identifier tables that bridge to each external system. It also names the **dispatcher**, the core seam that routes each parcel to its backend and drives submission. It specifies a reference backend set (`diracx-pilot`, `diracx-remote`, `htcondor`, `legacy-dirac`, and the special `recovery` pseudo-backend), the pull-vs-push distinction, and the boundary with [interCEde](https://github.com/DIRACGrid/intercede), which owns the Computing-Element/batch-system mechanics a backend delegates to. Data-transformation execution via **data backends** (requests) is specified in [DX-ADR-008](DX-ADR-008_data_management.md).

## Motivation

DiracX must run several backends concurrently:

- its **native pilot** workload system;
- a **push-style remote** backend for resources that cannot run pilots or call back (HPC, restricted sites), where work is pushed out and outcomes must be pulled;
- **HTCondor** glidein submission (the CMS model);
- a **legacy-DIRAC** bridge during migration;
- a **recovery** pseudo-backend that lets a partially-failed parcel register the output it *did* produce.

These differ in submission mechanics, in whether they push or pull outcomes, and in whether a DiracX job even exists on the other side. The transformation/parcel core must be blind to those differences and talk to all of them the same way.

## Specification

### Backend contract

Below is a conceptual interface, the exact details to be defined:

```python
class ComputeBackend:
    def __init__(self, config: dict) -> None:
        ...  # backend-specific configuration

    async def submit(self, payload: Payload, parcels: list[Parcel]) -> str:
        ...  # returns the backend's identifier, payload is common to all parcels for efficiency

    async def poll(self, parcels: list[Parcel]) -> list[ParcelOutcome]:
        ...  # and/or push callbacks

    async def retrieve(self, parcel: Parcel) -> ParcelOutcome:
        ...  # pull model

    async def cancel(self, parcel: Parcel) -> None:
        ...
```

- **submit** places the work (the parcel's materialised payload: its process, requirements and parameters, per DX-ADR-007/005) under the parcel's executing identity (the transformation's `ExecutingIdentity`, or the submitting user) and returns the backend's identifier, from which the dispatcher takes `Reserved → Assigned`.
- **poll / retrieve** report terminal outcomes together with the **output manifest**, the files produced under each CWL output id. One of those outputs is the job's **status report** (DX-ADR-007), which gives the **per-input outcomes**: processed, failed, not tried, or split into the portions that succeeded and the portions that did not. The core reads it while the parcel is `Completing`, builds the recovery split and the born-`Done` recovery parcel from it, and hands the inputs it reports as failed to the input failure hook of DX-ADR-006.
- Backends are part of the core, executed as [DX-ADR-001](DX-ADR-001_tasks.md) tasks (periodic sweeps and/or push callbacks). They are not an extension point (DX-ADR-006).

### Output registration

Every DiracX installation has a file catalogue, most often the DIRAC File Catalog or Rucio, and possibly more than one. Registering a parcel's outputs there happens while the parcel is `Completing` (DX-ADR-005), between the backend being finished with it and its outcome reaching the inputs. A VO registering into more than one catalogue registers into each. That state is re-entrant, so a registration that fails against one catalogue is retried rather than lost, and `Done` therefore means registered. The transition to `Done` records the files it produced (DX-ADR-004), which is what feeds the downstream transformations.

A parcel that fails never registers anything. Files it had already uploaded are not eligible for further processing and are removed by the transformation's archiving actions, so an abandoned upload is never mistaken for output.

### The dispatcher

The **dispatcher** is the connective seam between a parcel and its backend: the core component that turns an `Unassigned` parcel into submitted work. It claims `Unassigned` parcels, chooses the backend within whatever constraint the packer expressed, materialises the parcel's payload by resolving its stored-process references and pairing them with its resolved-hints sidecar (DX-ADR-007), calls that backend's `submit`, records the returned identifier in the backend's identifier table (DX-ADR-004), and advances `Reserved → Assigned`. Nothing else calls a backend's `submit`: the backend owns only what happens *outside* the database; the dispatcher owns the parcel↔backend handoff *inside* it. (Postally, the dispatch desk that hands each packed parcel to a carrier.)

The dispatcher is **core rather than a per-VO plugin**: routing is a lookup on `Backend`, and `Reserved` is the crash-safe intermediate state that bounds what a crash mid-submission can double-dispatch to a single parcel.

**The dispatcher chooses, the packer constrains.** A packer that knows a parcel must run somewhere specific says so and the dispatcher honours it; otherwise the choice is made at claim, which is what lets an installation move work between backends without touching any transformation. Ordering is deliberately not the dispatcher's concern: it claims in key order and hands work over as fast as the backends accept it. Priority is applied by whatever queue the work lands in, the matcher for `diracx-pilot` and the HTCondor queue for glidein resources, where the information needed to schedule it actually lives.

User jobs travel the same path: submission creates a user parcel in `Unassigned` (DX-ADR-004), so users reach every backend through the same dispatcher.

Claiming never joins: the parcel base table carries `Kind` (compute or data, denormalised) and optionally a forced `Backend`, so the dispatcher's claim is a flat indexed query whichever backend-selection policy applies (DX-ADR-004).

### Parcels are not retryable

All terminal parcel states (`Done`, `Failed`, `PartiallyDone`, `Cancelled`) are final. A failed user parcel is simply terminal, and resubmission is a new parcel. The `Reserved` state is the crash-safe step between "we decided to submit" and "the backend acknowledged", which bounds what a crash mid-submission can double-dispatch to a single parcel and marks it for inspection on restart; a submission the backend refuses returns the parcel to `Unassigned`, since no attempt was made. `Completing` is the step between the backend being finished with a parcel and the outcome being recorded on the inputs; pull-model backends fetch the outcome there, outputs are registered there, and a cancellation is confirmed there. (Full state machine in DX-ADR-005.)

### Backend identifiers

The only bridge between the parcel state machine and any backend's own state machine is the backend's identifier for the parcel (a slot id, a glidein id, a legacy job id), stored in a table per backend with a unique index on the identifier (DX-ADR-004). The DiracX WMS is not special: it has an identifier table like every other backend. The unique index supports the reverse lookup when a backend reports status ("HTCondor cluster 5678 finished, which parcel is that?").

### Examples of potential backends

| Backend         | Model                     | Notes                                                                                                                                                                                                                                                                                    |
| --------------- | ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `diracx-pilot`  | pull (pilots)             | Native: submission creates a WMS job from the parcel; pilots fetch and run it; outcomes pushed by the WMS.                                                                                                                                                                               |
| `diracx-remote` | push (PushJobAgent-style) | For resources that cannot run pilots or call back; the input object is resolved before the work is pushed out, and outcomes are fetched while the parcel is `Completing`. This should potentially be handled differently by consuming parcels from the `diracx-pilot` backend's matcher. |
| `htcondor`      | pull, push, or mixed      | CMS glidein submission; behaves like `diracx-pilot`, like `diracx-remote`, or a mix, per what the resource supports.                                                                                                                                                                     |
| `legacy-dirac`  | bridge                    | Submits to an existing DIRAC installation during migration; carries a shrinking fraction of parcels until it is retired (see below).                                                                                                                                                     |
| `recovery`      | none                      | A reserved *name*, not an implementation: born-`Done` parcels the core creates to claim the successfully-processed portions of a partial outcome. Nothing is submitted to it.                                                                                                            |

### Migrating off DIRAC

The `legacy-dirac` backend exists so that the switch to DiracX execution is gradual and reversible, and the dispatcher's freedom to choose a backend is what makes that possible. The transition runs in three steps:

1. Every parcel goes to `legacy-dirac`, which submits it to the existing DIRAC installation. DiracX owns the transformation bookkeeping; DIRAC still runs the work.
2. A fraction of parcels goes to a native backend instead, selected by hashing the parcel identifier. The split needs no coordination, is stable under retries of the dispatch, and is random within each transformation, so the two populations are comparable and a workgraph is never split along some structural line. The fraction is configuration, per installation and optionally per VO or transformation type.
3. The fraction rises as confidence grows, until DIRAC carries nothing and the backend is retired.

### Boundary with interCEde

The compute backend owns **policy**: matching, pilot management, outcome interpretation, recovery. It delegates the **mechanics** of talking to a Computing Element or batch system (submit, monitor and retrieve against ARC, HTCondor, Slurm-over-SSH or local) to interCEde. The seam between backend and interCEde is a first-class interface: a backend is policy plus an interCEde-driven CE, and does not re-implement scheduler chatter.

### Data backends

Data-transformation parcels are executed by **data backends** as **requests** (copy/delete). They share the parcel state machine, the dispatcher and the per-backend identifier tables, but carry no process: the parcel carries the request itself, written by the packer (DX-ADR-006) and stored in `DataParcels.Request` (DX-ADR-004). The backend submits the body it is given rather than translating it, because the request vocabulary is the DIRAC Request Management System's. [DX-ADR-008](DX-ADR-008_data_management.md) specifies the rest: the RMS backend, its identifier table, where retries live, and what the core knows about a request it does not interpret.

## Rationale

- **Backends behind one contract.** The transformation/parcel core touches only `submit/poll/retrieve/cancel` and the identifier tables; every resource-specific quirk lives behind the backend, so adding HTCondor or an HPC push model is a new backend rather than a change to the parcel core.
- **One identifier table per backend.** The DiracX WMS is just another backend with an identifier table of its own, so nothing in the parcel core is specific to the native path, and each backend's reverse lookup is a unique index on a column of the right type (DX-ADR-004).
- **Not-retryable parcels + recovery-as-a-backend.** Keeping each parcel immutable and letting recovery be a born-`Done` pseudo-backend preserves the invariant that each input is successfully processed by exactly one parcel, which external bookkeeping (output registration, deduplication) relies on. See DX-ADR-004 for the mechanics.
- **Materialisation at dispatch, not on the node.** Resolving stored-process references where the parcel is created means a worker node fetches nothing, needs no credentials for the process store, and runs offline on every backend rather than only on the ones that were designed for it. Because the payload is keyed on the process hash and the sidecar, a transformation's thousands of parcels share one materialisation.
- **A dispatcher that routes but does not schedule.** Giving the dispatcher a scheduling policy would put a third queue between the packer and the resources, competing with the ones the backends already have and needing its own notion of fairness. Claiming in key order keeps it a router, and a hash-based split over the same claim is what makes the DIRAC migration a configuration change rather than a fork in the code.
- **Delegating CE mechanics to interCEde.** CE/batch chatter is a distinct, independently-testable concern with its own backend matrix (ARC/HTCondor/Slurm). Keeping it in interCEde lets the compute backend be about policy, and lets the CE layer be reused (and shared) on its own.

## Rejected Ideas

- **Parcels as rows in the WMS `Jobs` table.** Divergent status vocabularies, and several backends have no DiracX job at all; the `Jobs` table is instead internal to the `diracx-pilot` backend (DX-ADR-004).
- **Retryable parcels / one-to-many recovery.** Breaks the exactly-one-parcel invariant; see DX-ADR-004.
- **A single, hard-coded backend.** The DIRAC assumption; incompatible with concurrent heterogeneous resources.
- **Re-implementing CE/batch submission inside each backend.** Duplicates interCEde and forfeits its real-backend test matrix.
- **Extendability of backends.** All backends must be defined within DiracX itself. It is not foreseen that there could be a community specific-backend that would have no use outside of that community, and the complexity of a plugin system is not justified.

## Open Issues

- **Push vs pull details.** The exact callback protocol, the retrieval cadence while `Completing`, and idempotency guarantees on broker restart (per DX-ADR-001) need specifying.
- **Backend capability negotiation.** How a transformation discovers whether its backend pushes, pulls, or both, and how the backend's raw status is surfaced, is open.
- **interCEde interface mapping.** The concrete mapping of the backend contract onto interCEde's submit/monitor/retrieve API (and its `JobResource` model) needs pinning as interCEde stabilises.
- **A native request vocabulary.** Data parcels carry DIRAC RMS request bodies. What replaces them is tracked in DX-ADR-008, with which it is shared.
