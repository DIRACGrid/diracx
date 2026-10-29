# Transformation feeders

Feeders propose new transformation inputs. Plugins only yield proposals; the
core materialises them into `Inputs` / `FeederState` / `FeederEnabled`
([DX-ADR-006](../../../adr/DX-ADR-006_extensions.md)). Schema and uniqueness
are in [DX-ADR-004](../../../adr/DX-ADR-004_schema.md); when feeders run is in
[DX-ADR-005](../../../adr/DX-ADR-005_state_machines.md).

## Core loop

```text
PeriodicSweepFeeders (periodic)
  → list Active + FeederEnabled + Feeder IS NOT NULL
  → schedule one FeedTransformation(transformation_id) each

FeedTransformation
  → MutexLock(transformation, id, "feeder")
  → load feeder entry point; validate FeederArgs
  → run plugin (scouting=False)
  → materialise_feeder_yields(...)
```

Materialisation inserts `Unassigned` inputs in configurable chunks, updates
`FeederState` on `SetFeederState`, and clears `FeederEnabled` on
`NoMoreInputs`. `InputHash` (XXH3-128 over canonical `(LFN, Descriptor)`)
deduplicates inserts, so yielding ahead of the bookmark is safe on restart.

Enable the sweep with `DIRACX_TASKS_FEEDER_ENABLED=true` (interval:
`DIRACX_TASKS_FEEDER_INTERVAL_SECONDS`, default 60).

## Register a feeder

1. Implement a **sync generator** with fixed kwargs `feeder_args` (pydantic
    model), `feeder_state` (`dict`), and `scouting` (`bool`).
2. Yield `Input`, `SetFeederState`, and/or `NoMoreInputs` from
    `diracx.core.models.feeder`.
3. Register under `diracx.transformations.feeders` (core plugins live in
    `diracx.logic.transformations.feeders`).
4. Set `Transformations.Feeder` to the entry-point name and store JSON in
    `FeederArgs`.

Built-in example: `IntegerFeeder` (`max_count` in args; bookmark `next_seed`).

```toml
[project.entry-points."diracx.transformations.feeders"]
MyFeeder = "mypackage.feeders.mine:my_feeder"
```

```python
from collections.abc import Generator
from typing import Any

from pydantic import BaseModel, NonNegativeInt

from diracx.core.models.feeder import Input, NoMoreInputs, SetFeederState


class MyFeederArgs(BaseModel):
    max_count: NonNegativeInt = 0


def my_feeder(
    *,
    feeder_args: MyFeederArgs,
    feeder_state: dict[str, Any],
    scouting: bool,
) -> Generator[Input | SetFeederState | NoMoreInputs, None, None]:
    ...
```

`FeederArgs` in the database is opaque JSON. Only the plugin's pydantic model
defines its shape. `FeederState` is likewise opaque to the core (bookmark).

## Future work

Deferred to keep the Feeder MVP small ([#1082](https://github.com/DIRACGrid/diracx/issues/1082)):

1. **Edge feeder** — feed downstream transformations from `ParcelOutputs`
    ([#1087](https://github.com/DIRACGrid/diracx/issues/1087)); cascade drain
    along the DAG.
2. **Metadata catalogue / bookkeeping feeder** — VO catalogue queries,
    watermarks, approximate bookmarks + reconciliation.
3. **Multi-transformation workgraphs** — more than one member; workgraph-level
    drain/halt; Active/Scouting hooks adjusting `FeederArgs`.
4. **Scouting mode** — `scouting=True`, reduced targets, `ScoutingToApproving`
    extending feeder args.
5. **Packer** — claim Unassigned, parcels, `feeder_active` / flush behaviour.
6. **Hooks and actions** — transformation/workgraph Active hooks,
    `HandleFailedInput`, Finalizing (`ReconcileFeederInputs`), Archiving,
    Cleaning.
7. **CWL / REST submission path** — populate `Feeder` / `FeederArgs` from
    hints ([#1086](https://github.com/DIRACGrid/diracx/issues/1086)).
8. **RateLimiter / ConcurrencyLimiter tuning** — global caps on concurrent
    feed tasks or catalogue QPS once real feeders exist.
9. **Input counter journals (ADR-009)** — exact Unassigned counts without
    table scans.
10. **Plugin discovery versioning** — pin feeder plugin version per
    transformation (ADR-006 open issue).
11. **Workgraph drain polling task** — core check that every member is drained
    → Finalizing.
12. **File-oriented feeder features** — masks, duplicate-file-via-descriptor
    rounds, size-based packing helpers.
13. **Event-accounting seed feeder** — stop when produced + in-flight events
    cover the target (full ADR `seed_feeder`), not only `max_count`.
14. **Async feeder plugins + extra dependency injection** — ADR-006 allows
    injected DBs/settings beyond the fixed kwargs; MVP feeders are sync
    generators with `feeder_args` / `feeder_state` / `scouting` only.
15. **Feeder ↔ packer lock coordination** — revisit mutex / shared status lock
    keys once the packer lands so both sides agree.
16. **Re-check `Active` at feed-task start** — sweep already filters; harden
    `FeedTransformation` against status races.
