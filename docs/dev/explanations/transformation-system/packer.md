# The packer

The packer is the part of the Transformation System that turns a transformation's `Unassigned` inputs into parcels. It is split in two, following [DX-ADR-006](../../../adr/DX-ADR-006_extensions.md):

- a **packer plugin**, chosen per transformation by its `Packer` column, decides how inputs are grouped;
- the **core**, `pack_transformation` in `diracx.logic.transformations`, claims the inputs, checks what the plugin proposes and is the only thing that writes to the database.

One rule applies throughout: **plugins propose, the core writes.**

## How a packer run works

`PackerSweepTask` runs periodically and schedules one `PackTransformationTask` for each `Active` transformation. Each of those tasks holds a packer mutex on its transformation id, so two runs for one transformation never overlap while different transformations, and the transformation's own feeder, run in parallel. It also holds the shared side of the transformation's `status` reader-writer lock; anything that changes the status takes the exclusive side, so a Pause waits for a run in progress to finish and the next run sees it. A run is one database transaction:

1. The core loads the transformation. A transformation that is not `Active` is skipped.
2. It resolves the plugin from the `diracx.transformations.packers` entry point named by `Packer`, and validates `PackerArgs` against the plugin's arguments model.
3. It claims `Unassigned` inputs whose `DelayedUntil` has passed, with `SELECT ... FOR UPDATE SKIP LOCKED`, in pages of 1000 and up to `DIRACX_TASKS_PACKER_MAX_INPUTS_PER_INVOCATION`, and streams them to the plugin.
4. It checks and applies each proposal the plugin yields, writing parcels in batches.
5. Inputs the plugin did not use stay `Unassigned`; their row locks are released when the transaction commits.

If any check fails, the task fails and the whole transaction rolls back, so a run either applies everything the plugin proposed or nothing.

## The plugin contract

A packer is a generator function, preferably asynchronous, called with keyword arguments only:

| Argument         | Type                     | Meaning                                                                               |
| ---------------- | ------------------------ | ------------------------------------------------------------------------------------- |
| `packer_args`    | a pydantic model         | The transformation's `PackerArgs`, validated against the annotation of this parameter |
| `inputs`         | stream of `PackerInput`  | The claimed inputs, in `InputID` order                                                |
| `flush`          | `bool`                   | Pack groups that would otherwise be held back as too small                            |
| `feeder_active`  | `bool`                   | Whether more inputs may still arrive: the transformation has an enabled feeder        |
| `input_overview` | `dict[InputStatus, int]` | How many of the transformation's inputs are in each status                            |

`PackerInput` carries `input_id`, `parent_input_id`, `lfn`, `lfn_size`, `descriptor` and `error_count`.

The plugin yields proposals, all defined in `diracx.core.models.transformation`:

| Proposal        | Effect                                                                                                                                                                                                                                               |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ComputeParcel` | A new `Unassigned` parcel made of the given inputs, which move to `Assigned`. Its `requirements` are merged over the transformation's requirements template; `parameters` is the CWL input object; `metadata` is stored as the parcel's annotations. |
| `BadInput`      | The input moves to `Problematic`, and the reason is recorded as an `InputProblematic` event in `TransformationLog`.                                                                                                                                  |
| `DelayInput`    | The inputs get `DelayedUntil` and are not offered again before that time.                                                                                                                                                                            |
| `DataParcel`    | Not supported yet.                                                                                                                                                                                                                                   |

### What the core checks

- Every input in a proposal must have been offered to the plugin in this run.
- No input may appear in more than one proposal in a run.
- Every input must still be `Unassigned` when it is written, and the move must be allowed by the input state machine of [DX-ADR-005](../../../adr/DX-ADR-005_state_machines.md).
- `PackerArgs` must validate against the plugin's arguments model.

### Using `flush` and `feeder_active`

Deciding when to pack a small remainder is the plugin's job, not the core's. While `feeder_active` is true, more inputs may arrive, so a packer that groups inputs can hold back an incomplete group. Once the feeder is off, or when `flush` is set, it should pack what is left, otherwise the transformation never finishes. `input_overview` is there for packers whose thresholds depend on how much of the pool is left.

## Writing and registering a packer

A packer that groups inputs by run number:

```python
from collections import defaultdict
from collections.abc import AsyncGenerator, AsyncIterator

from pydantic import BaseModel, PositiveInt

from diracx.core.models.transformation import (
    BadInput,
    ComputeParcel,
    InputStatus,
    PackerInput,
)


class ByRunPackerArgs(BaseModel):
    group_size: PositiveInt = 10


async def by_run_packer(
    *,
    packer_args: ByRunPackerArgs,
    inputs: AsyncIterator[PackerInput],
    flush: bool = False,
    feeder_active: bool,
    input_overview: dict[InputStatus, int],
) -> AsyncGenerator[ComputeParcel | BadInput, None]:
    groups = defaultdict(list)
    async for packer_input in inputs:
        run = packer_input.descriptor.get("Run")
        if run is None:
            yield BadInput(input_id=packer_input.input_id, reason="No run number")
            continue
        groups[run].append(packer_input.input_id)
        if len(groups[run]) == packer_args.group_size:
            yield ComputeParcel(inputs=groups.pop(run), metadata={"Run": run})
    if flush or not feeder_active:
        for run, group in groups.items():
            yield ComputeParcel(inputs=group, metadata={"Run": run})
```

Register it in your extension's `pyproject.toml`:

```toml
[project.entry-points."diracx.transformations.packers"]
ByRun = "myext.logic.transformations.packers:by_run_packer"
```

A transformation then uses it with `Packer = "ByRun"` and, for example, `PackerArgs = {"group_size": 20}`. If several installed extensions register the same name, the one with the highest extension priority wins, so an extension can replace a packer shipped by DiracX.

A synchronous generator function also works. Since it cannot wait for the database between pages, it is offered all the claimed inputs at once, which `DIRACX_TASKS_PACKER_MAX_INPUTS_PER_INVOCATION` keeps bounded.

DiracX ships one packer, `FixedSize`, which makes parcels of `group_size` inputs in the order they are offered and holds back a smaller last group until the feeder is off or a flush is requested.

## Configuration

The tasks are configured with environment variables, listed in the [environment variables reference](../../../admin/reference/env-variables.md#packersettings):

- `DIRACX_TASKS_PACKER_ENABLED` turns the periodic sweep on. It is off by default.
- `DIRACX_TASKS_PACKER_INTERVAL_SECONDS` sets how often the sweep runs.
- `DIRACX_TASKS_PACKER_MAX_INPUTS_PER_INVOCATION` caps the inputs offered to a packer in one run.

## Current limitations

- `DataParcel` proposals and transformations of the Data kind raise `NotImplementedError`.
- The journalled counters of DX-ADR-009 are not written yet, and `input_overview` is computed with a `GROUP BY` over `Inputs` ([#1085](https://github.com/DIRACGrid/diracx/issues/1085)).
- Plugins receive only the arguments above. Injecting databases and settings into them, as DX-ADR-006 describes, is not implemented yet.
