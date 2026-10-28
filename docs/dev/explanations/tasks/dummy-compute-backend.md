# Dummy compute backend

The dummy compute backend gives every compute parcel of the Transformation
System a successful outcome without running anything. It lets the parcel
lifecycle be exercised before the dispatcher and the real compute backends
exist.

It walks each parcel through the parcel state machine of
[DX-ADR-005](../../../adr/DX-ADR-005_state_machines.md), standing in for two
components of [DX-ADR-003](../../../adr/DX-ADR-003_compute_backends.md):

- the **dispatcher**, which claims a parcel: `Unassigned → Reserved → Assigned`;
- a **compute backend**, which reports the outcome: `Assigned → Completing → Done`.

Reaching `Done` moves the parcel's inputs from `Assigned` to `Processed` in the
same transaction.

## How it works

Two tasks in `diracx.tasks.transformations` share the work:

- `DummyComputeBackendMonitorTask` runs periodically. It claims a batch of
    `Unassigned` compute parcels, oldest first, that have no backend or the
    `dummy` one, and moves them to `Assigned` with `Backend = "dummy"`. After
    committing, it schedules one `DummyComputeBackendTask` per parcel,
    `runtime_seconds` later.
- `DummyComputeBackendTask` moves its parcel from `Assigned` to `Done`, holding
    a mutex on the parcel.

The transitions go through `diracx.logic.transformations.set_parcel_status`,
which checks them against the state machine and applies the outcome to the
inputs. In the database they are compare-and-set: only parcels still in the
expected status move. Two monitors running at once, or a retried completion
task, therefore never move a parcel twice.

The settings are documented with the
[environment variables](../../../admin/reference/env-variables.md), and
[Run a task manually](../../../admin/how-to/tasks/run-task-manually.md) shows
how to run both tasks by hand.

## Limitations

- Nothing creates parcels yet, as there is no packer.
- Every parcel succeeds and produces no output. `PartiallyDone` is not
    supported until the recovery split exists.
- If the monitor dies after its commit and before it has scheduled every
    completion task, those parcels stay `Assigned`. Reconciling them is an
    open issue of DX-ADR-003 for every backend.
- When the dispatcher exists, the claim moves there and a backend keeps only
    its submission and its outcome reporting.
