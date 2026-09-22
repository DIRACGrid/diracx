# DX-ADR-006: Transformation System extension points

## Metadata

- **Created By:** Chris Burr, Christophe Haen
- **Date:** 2026-07-10
- **Status:** Draft
- **Decision Maker(s):** TBD

## Abstract

The Transformation System core is generic. Everything experiment-specific, from how inputs are found and grouped to what counts as a finished workgraph, lives in extensions that fill a fixed set of extension points. There are four kinds: the **feeder** and the **packer** that shape a transformation's input pool, **hooks** that run while an entity is in a state and return what the core should change, and **actions**, ordered lists of checks and steps that record a result each. A workgraph has a hook that decides when scouting is done, a hook that runs while it is active, and the list of actions that approve it. A transformation has its feeder and packer, a hook that runs while it is active, a hook that decides the fate of a failed input, and the lists of actions that finalise, archive and clean it. Backends are not an extension point. State machines are also not customisable by extensions.

Every extension is a plain function discovered through an entry point and run as a [DX-ADR-001](DX-ADR-001_tasks.md) task, with its database and configuration dependencies injected the same way as for any other task. One rule applies throughout: **plugins propose, the core writes.** A plugin returns a description of what should happen; only the core turns that into rows, inside one transaction. The signatures below fix responsibilities and data flow; the concrete Python API is fixed at implementation time.

## Motivation

Mask semantics, grouping policy, failure policy, approval criteria and the checks that make a workgraph complete all differ per community. Putting any of them in the core would either impose one experiment's conventions on everyone or grow core code the core cannot reason about. The old system solved this with plugin classes and agents that each opened their own transactions and wrote their own state, which is where the bookkeeping drift and races of DX-ADR-001's motivation came from. A narrow plugin surface with a single writer keeps the flexibility and removes the drift.

## Specification

### Common rules

- Extensions are functions registered through entry points. Their parameters beyond the ones each extension point defines are injected by the task framework of DX-ADR-001: a parameter annotated with a database, settings or service class receives an instance, so an LHCb feeder can ask for the bookkeeping database by annotating an argument with its class.
- **Plugins propose, the core writes.** A plugin never touches entity tables. It returns proposals, the core materialises each batch of them (rows, transitions, journal entries) in one transaction, and a plugin that raises leaves nothing half-written. An extension that raises is a task failure, surfaced by the retry policy and monitoring of DX-ADR-001, and not a state of the workgraph or of the member.
- **One invocation per transformation at a time.** A periodic sweep enqueues one task per active transformation rather than one task for all of them, and that task holds a mutex keyed on the transformation id. DX-ADR-001's locks already take a key, so this needs no new machinery, but it is what makes a feeder's bookmark and a packer's claim safe: two invocations for one transformation never overlap, while thousands of transformations still run in parallel.
- **Hooks and actions run in states.** Each is bound to a state of the owning entity and runs while the entity is in it; transitions themselves are plain transactions (DX-ADR-005).
- **Hooks return an operation or a decision.** The `ScoutingToApproving` and `Active` hooks return operations, changes to a transformation's feeder or status: a workgraph hook returns a dict of transformation id to operation, a transformation hook a single operation. The `HandleFailedInput` hook returns a decision, the fate of one input. An operation and a decision are different types.
- **Monitoring does not go in a hook.** A hook may read whatever its own decision needs, which is why the simulation workgraph's `Active` hook queries the bookkeeping for the events produced. What does not belong in `ScoutingToApproving` or in either `Active` hook is work done for something else to look at: emitting metrics, refreshing a dashboard, or computing a summary nothing in the hook's own return value depends on. The sweep that runs a hook holds the mutex for the entity it is sweeping, so that work is time the feeder and packer spend queued behind it. A hook that raises is a task failure, so an unreachable metrics endpoint would present as a failure to control the workgraph and be retried as one. And a hook bound to `Active` or `Scouting` is not called once the entity leaves that state, so whatever it published stops being updated as soon as the workgraph finishes. Counts come from the counters of [DX-ADR-009](DX-ADR-009_counters.md), which any process can read exactly and without holding a lock, and the transition history is in the log tables (DX-ADR-004).
- **Actions are ordered lists with persisted results.** An action list is a set of rows ordered by position (DX-ADR-004), run one at a time. The runner takes the lowest position with no result yet. An action returns `Passed` or `Done`, `Failed`, or `Pending` when the answer is not yet knowable, such as parcels that are still cancelling; `Pending` leaves the entity where it is and the action is tried again. A sign-off nobody has given is `Failed`, not `Pending`: the person who signs off forces the action to passed. A `Failed` action moves the entity to the state's blocked counterpart, where an operator forces the result or resets it to run again. Entering a state resets that state's results in the same transaction as the transition.
- Bindings come from the CWL hints (DX-ADR-007), with defaults from the configuration service resolved by the workgraph's and transformation's `Type`.

| Owner          | Kind   | Extension point       | Runs while                      | Returns                                                                   |
| -------------- | ------ | --------------------- | ------------------------------- | ------------------------------------------------------------------------- |
| Workgraph      | Hook   | `ScoutingToApproving` | `Scouting`                      | accept, or operations that extend the scout with new feeder arguments     |
| Workgraph      | Hook   | `Active`              | `Active`                        | operations: changed feeder arguments, disabled feeders, a member to pause |
| Workgraph      | Action | `Approving`           | `Approving`                     | passed, failed or pending, with updated requirements                      |
| Transformation | Feeder |                       | `Active`, while `FeederEnabled` | new inputs, an updated bookmark, exhaustion                               |
| Transformation | Packer |                       | `Active`                        | parcels, rejected inputs, delayed inputs                                  |
| Transformation | Hook   | `Active`              | `Active`                        | an operation: changed feeder arguments, feeder disabled, pause, flush     |
| Transformation | Hook   | `HandleFailedInput`   | an input is `Failed`            | a decision: `Unassigned`, `Split` or `Problematic`                        |
| Transformation | Action | `Finalizing`          | `Finalizing`                    | passed, failed or pending                                                 |
| Transformation | Action | `Archiving`           | `Archiving`                     | done, failed or pending                                                   |
| Transformation | Action | `Cleaning`            | `Cancelling`                    | done, failed or pending                                                   |

The scouting phase of a workgraph runs the feeder and the packer of its active members in the same way as the active phase; the feeder is told which phase it is in.

### Feeder

The feeder is the bridge to the experiment's metadata catalogue, or to whatever else produces a transformation's inputs. It yields inputs; the core inserts them as `Unassigned` (DX-ADR-005).

A feeder also yields its own **bookmark**, which the core stores in `FeederState` (DX-ADR-004) in the same transaction as the inputs that preceded it. That is what stops the feeder *producing* an input it has already produced. It is not what stops the duplicate *row*: the core hashes each input's `(lfn, descriptor)` pair and the hash is unique within the transformation (DX-ADR-004), so a repeat is refused on insert whatever the feeder does. Hashing the descriptor rather than the LFN alone is what lets that key survive splitting, where several rows legitimately share an LFN. The bookmark is therefore about work rather than correctness, and the work is worth saving: without one a catalogue feeder re-reads the whole catalogue on every sweep to have all but the newest rows thrown away.

A bookmark is allowed to be approximate, and the shape depends on the feeder:

- **A watermark**, the timestamp of the last query, for a catalogue feeder. It can miss a file that committed after the query ran but carries an earlier stamp.
- **A cursor**, the highest seed issued, for a simulation feeder. This one is exact.
- **A Bloom filter** over what has been yielded, when nothing cheaper fits. A false positive skips an input that was never inserted.

The misses are caught by a **reconciliation action** in the transformation's `Finalizing` list: it re-evaluates the query with no bookmark and fails if anything is missing, so an approximate bookmark costs a full pass once per workgraph rather than correctness. The action reports what is missing and does not repair it. Recovering a miss is an operator's work: each one is its own special case in practice, and automating the repair inside the loop is what DIRAC showed to be difficult.

```python
class ExampleFeederArgs(BaseModel):
    ...


class Input:
    descriptor: dict[str, JSONSerializable]


class FileInput(Input):
    lfn: str
    lfn_size: int


class SetFeederState:
    state: dict[str, JSONSerializable]


class NoMoreInputs:
    ...


def example_feeder(
    *,
    feeder_args: ExampleFeederArgs,
    feeder_state: dict[str, JSONSerializable],
    scouting: bool,
    # remaining arguments are injected
    config: Config,
    bk_db: BookkeepingDB,
) -> Generator[Input | FileInput | SetFeederState | NoMoreInputs, None, None]:
    ...
```

- `feeder_args` is the transformation's `FeederArgs` column, validated against the feeder's own model. The column is written from the `dirac:Feeder` hint at submission and can be changed later by the `ScoutingToApproving` hook and the `Active` hook. It is validated for the phase the document implies as well as against the model, so that arguments valid outside a scout and contradictory inside one are refused at submission rather than at the first sweep: the `SamplingFraction` below stays the plugin's own guard, and the submission check is what stops a document reaching it.
- `scouting` tells the feeder which phase the workgraph is in. Branching on it should be minimal, so that what runs during scouting stays representative of what runs afterwards.
- `SetFeederState` commits with the inputs yielded before it, so a feeder that yields it after each batch is restartable at batch granularity.
- `NoMoreInputs` clears `FeederEnabled`, which is one of the conditions for the transformation to drain (DX-ADR-005).
- `lfn_size` is the size of the whole file, whatever a mask in `descriptor` selects. A feeder that wants the size of the selected fraction puts it under its own key in `descriptor`. A size-based packer that groups several masks of one LFN into one parcel can still account for the transfer by summing over distinct LFNs.
- **A feeder whose source can produce the same file twice says so in the descriptor.** The core refuses a duplicate on a hash of `(lfn, descriptor)` (DX-ADR-004), so two yields of one file are two inputs only if their descriptors differ. A removal feeder for a standalone data transformation is the case: a file staged to disk at a user's request is removed, staged again later, and has to be removed again. It carries whatever identifies the round, the staging request or a cycle number, under its own key, and each round becomes a separate input with its own lifecycle. A feeder that puts nothing there gets the ordinary behaviour, where a file it has already yielded is dropped on insert.

#### The edge feeder

An internal edge of the workgraph is served by the **edge feeder**, which DiracX provides and which needs no experiment catalogue at all. Its argument is the declared output it reads from, the `TransformationOutputID` compiled from the step's `in` source (DX-ADR-004), and it yields the producer's `ParcelOutputs` rows as file inputs. Its bookmark is the largest `OutputID` it has fed, so each invocation is a primary-key range scan from the bookmark, and the producer never needs to know who reads its output. An id is minted before its row commits, so the feeder trails the present by a margin and, like a catalogue feeder, relies on the reconciliation action at `Finalizing`, which compares the producer's rows against the inputs by LFN. It has no scouting branch, since the upstream feeder already decided the sample. It reports exhaustion once the upstream transformation is drained in the sense of DX-ADR-005, its feeder disabled and none of its inputs or parcels non-terminal, and its bookmark has reached the last row. A `Failed` upstream input awaiting its handler has no live parcel but is not terminal, and neither is a `Problematic` one awaiting an operator, so parcels alone are not enough. This is how `FeederEnabled` clears along a chain of transformations one after another, and it is why the close of a workgraph is a cascade: disabling the first feeder lets its packer pack the remainder, its drain disables the next feeder, and so on down the DAG, each packer becoming aggressive only once its own feeder is off. A step input sourced from another step gets the edge feeder unless it carries a `dirac:Feeder` hint naming another (DX-ADR-007). A step with several inputs sourced from other steps has one driving edge, named by its `dirac:Transformation` hint: the edge feeder serves that edge only, and the packer joins the others (see the joining packer below).

#### Examples

A simulation feeder has no catalogue to query. It yields seeds until the number of events requested is covered by what has been produced and what is in flight, and during scouting the target is the scouting sample instead. Its bookmark is the highest seed it has issued:

```python
def seed_feeder(
    *,
    feeder_args: SeedFeederArgs,
    feeder_state: dict[str, JSONSerializable],
    scouting: bool,
    config: Config,
):
    event_target = feeder_args.target_number_of_events
    if scouting:
        event_target = feeder_args.scouting_events or config.seed_feeder.scouting_events
    next_seed = feeder_state.get("next_seed", 0)
    while event_target > generated_events + inflight_events:
        yield Input(descriptor={"seed": next_seed})
        next_seed += 1
    yield SetFeederState(state={"next_seed": next_seed})
```

A bookkeeping feeder evaluates a catalogue query. During scouting it asks for a sample; afterwards it avoids re-running the full query on every invocation by asking only for files registered since its last check, with a full run once a day:

```python
def bookkeeping_feeder(
    *,
    feeder_args: BookkeepingFeederArgs,
    feeder_state: dict[str, JSONSerializable],
    scouting: bool,
    config: Config,
    bk_db: BookkeepingDB,
):
    bk_query = feeder_args.bk_query
    if scouting:
        if "SamplingFraction" in bk_query:
            raise ValueError(
                "SamplingFraction is not supported for transformations with scouting=True"
            )
        bk_query["SamplingFraction"] = (
            feeder_args.scouting_fraction or config.bookkeeping_feeder.scouting_fraction
        )
    else:
        # If we're not scouting avoid running the full query too often
        last_full_run = feeder_state.get("last_full_run", 0)
        if now() - last_full_run < 24 * 60 * 60:
            bk_query["InsertTimeAfter"] = feeder_state["last_check"]
        else:
            last_full_run = now()
    started_at = now()
    yield from bk_db.query(bk_query)
    yield SetFeederState(
        state={"last_full_run": last_full_run, "last_check": started_at}
    )
```

The watermark is the time the query *started*, not the time it finished, and it is still allowed to miss a file whose insert time was stamped before the query ran but which committed after it. That is what the reconciliation action exists for.

### Packer

The packer decides when and how `Unassigned` inputs become parcels, and nothing else. One invocation receives a stream of claimed `Unassigned` rows (`FOR UPDATE SKIP LOCKED`, excluding rows whose `DelayedUntil` has not passed), the transformation's packer arguments, and enough context to decide whether to wait for more input. It yields proposals as a generator, so memory stays bounded for transformations with millions of inputs and the core can commit in batches.

```python
class ExamplePackerArgs(BaseModel):
    ...


class ComputeParcel:
    inputs: list[UUID]
    metadata: dict[str, JSONSerializable]
    # merged over the transformation's requirements template
    requirements: Requirements | None
    # the CWL input object passed to the runner
    parameters: dict[str, JSONSerializable] = {}


class StorageDelta:
    se: str
    # both signed: added at this storage element if positive, freed if negative
    files: int
    bytes: int


class DataParcel:
    inputs: list[UUID]
    metadata: dict[str, JSONSerializable]
    # a DIRAC RMS request body (DX-ADR-003)
    request: dict[str, JSONSerializable]
    # what the request costs each storage element, declared rather than parsed out of it
    deltas: list[StorageDelta]


class BadInput:
    input_id: UUID
    reason: str = "Unknown reason"


class DelayInput:
    input_ids: list[UUID]
    delay_until: datetime


def example_packer(
    *,
    packer_args: ExamplePackerArgs,
    inputs: Generator[PackerInput, None, None],
    flush: bool = False,
    feeder_active: bool,
    input_overview: dict[Status, int],
    # remaining arguments are injected
    config: Config,
    bk_db: BookkeepingDB,
) -> Generator[DataParcel | ComputeParcel | BadInput | DelayInput, None, None]:
    ...
```

- A `ComputeParcel` or `DataParcel` becomes a parcel row, its facet row, the `ParcelInputs` links, the journal rows, and the `Assigned` transition of its inputs, all in one transaction (DX-ADR-004). The requirements of a compute parcel are merged over the transformation's template and stored content-addressed. The request of a data parcel is what the data backend hands to the request system, and its `deltas` are the storage elements it acts on and the signed files and bytes it costs each of them: the packer states all of it rather than leaving the core to parse an RMS body whose vocabulary DX-ADR-008 already wants to replace. It states the two counts rather than letting the core derive them from the parcel's inputs because only the packer knows whether several of those inputs are masks of one file, which would be counted once each way. Each delta becomes a `DataParcelDeltas` row and a data-counter journal row, the core routing it by its sign into the added or the freed columns (DX-ADR-004), so a parcel replicating to three destinations counts against all three and one that removes counts against what it frees.
- A `BadInput` is one the packer cannot use at all; the core moves it to `Problematic` with the reason (DX-ADR-005).
- A `DelayInput` sets `DelayedUntil` on the inputs and changes nothing else; they are offered to the packer again after that time.
- `flush` asks the packer to make parcels out of groups it would otherwise hold back as too small (DX-ADR-005).
- **`feeder_active` and `input_overview` are what let a transformation finish.** They tell the packer whether more input is coming and how the pool is distributed, so a packer that groups by size can drop its threshold as the pool dries up and pack the remainder once the feeder is off. Draining is deliberately the packer's decision rather than a core rule, because only the packer knows whether a small parcel is worth submitting; the core only checks the result (DX-ADR-005).

Splitting is deliberately not a packing concern: splits on failure come from the job's status report (DX-ADR-007) or from the `HandleFailedInput` hook below, and upfront chunking is just the feeder yielding several pre-masked inputs for one LFN.

#### Examples

Raw data distribution assigns each run to a destination the first time it sees the run and keeps the assignment for the rest of the transformation, so that a run's files stay together. Different transformations use different assignments, named by a packer argument. The assignment table belongs to the extension:

```python
class RunAssignmentPackerArgs(BaseModel):
    run_assignment_id: str


def run_assignment_packer(
    *,
    packer_args: RunAssignmentPackerArgs,
    inputs: Generator[PackerInput, None, None],
    config: Config,
    db: TransformationDB,
) -> Generator[DataParcel, None, None]:
    for input in inputs:
        destination = db.get_run_assignment(
            packer_args.run_assignment_id, input.descriptor["run"]
        )
        if destination is None:
            destination = db.assign_run(
                packer_args.run_assignment_id, input.descriptor["run"]
            )
        yield DataParcel(
            inputs=[input.id],
            metadata={"destination": destination},
            request=replicate_request(input.lfn, destination),
            deltas=[StorageDelta(se=destination, files=1, bytes=input.lfn_size)],
        )
```

Comparing two reconstructions of the same files joins two edges. The pool holds the outputs of reconstruction A, fed along the driving edge; the outputs of reconstruction B are recorded under its own declared output and never fed to this transformation. The packer finds each input's companion through the schema (DX-ADR-004): the parcel that produced the input, that parcel's inputs, reconstruction B's input for the same source file, and the files its parcel produced. A partner that does not exist yet delays the input, and one that never will marks it `Problematic`.

```python
class JoinPackerArgs(BaseModel):
    partner: UUID  # TransformationOutputID of the joined output
    partner_input: str  # the step input the partner fills


def join_packer(
    *,
    packer_args: JoinPackerArgs,
    inputs: Generator[PackerInput, None, None],
    config: Config,
    db: TransformationDB,
) -> Generator[ComputeParcel | BadInput | DelayInput, None, None]:
    for input in inputs:
        partner = db.find_partner_output(input, output=packer_args.partner)
        if partner is None:
            if db.partner_input_status(input, packer_args.partner) == "NotProcessed":
                yield BadInput(input.id, reason="partner will never be produced")
            else:
                yield DelayInput([input.id], delay_until=now() + timedelta(minutes=15))
            continue
        yield ComputeParcel(
            inputs=[input.id],
            metadata={},
            requirements=None,
            parameters={packer_args.partner_input: partner.lfn},
        )
```

The RDST stripping packer has the same shape with an external lookup: the partner of each RDST is its RAW ancestor from the bookkeeping, which is staged rather than produced, so the delay waits on a staging transformation instead of on a parcel.

Moving an analysis production to the archive means ensuring a tape replica exists and then removing the disk replicas. The packer looks at where each file is and yields a different parcel for each case:

- the LFN does not exist or is not on the expected storage element: a `BadInput`;
- already on tape and not on disk: a parcel with an empty request and no deltas;
- already on tape and on disk: a parcel that only removes, with a negative delta at the disk storage element;
- on disk and not on tape: a parcel that replicates and then removes, with a positive delta at the tape storage element and a negative one at the disk storage element.

### Hooks

A hook is a function the core calls with the state of an entity, and whose return value the core applies. The `ScoutingToApproving` and `Active` hooks return operations; the `HandleFailedInput` hook returns a decision.

An **operation** is a change to one transformation: new feeder arguments, the feeder enabled or disabled, the transformation paused, or its packer flushed (DX-ADR-005). `pause` and `flush` are instructions rather than settings, which is why they are booleans that only act: nothing is persisted, and a flush is one invocation of the packer with `flush=True`. A workgraph hook returns a dict of transformation id to operation, so one invocation can act on several members; a transformation hook returns one operation for itself.

```python
class Operation:
    feeder_args: dict[str, JSONSerializable] | None = None
    feeder_enabled: bool | None = None
    pause: bool = False
    flush: bool = False
```

A **decision** is the fate of one `Failed` input: back to `Unassigned`, to `Problematic`, or split into children.

```python
class ToUnassigned:
    ...


class ToProblematic:
    reason: str


class SplitInto:
    children: list[Input | FileInput]


Decision = ToUnassigned | ToProblematic | SplitInto
```

#### `ScoutingToApproving`

Runs periodically while the workgraph is `Scouting` and decides whether enough has been done. It either accepts, which moves the workgraph to `Approving`, or extends the scout by returning operations that give some members new feeder arguments. A filtered simulation, where the reconstruction keeps a small fraction of the events, extends the scout until enough events have survived, up to a limit beyond which it accepts anyway.

The hook may also climb a ladder rather than run one sample: submit a handful of parcels, and once they have run extend the feeders to the next stage, up to the last, at which it accepts. Accepting is also how the hook reports a scout in trouble. Once enough of the sample has failed it accepts at once, knowing that `CheckSuccessRate` will fail: the workgraph moves to `ApprovingBlocked`, which is the one place an operator looks, and a scout still running is plainly `Scouting`. For that to be a guarantee rather than a hope, the hook and the check judge one threshold, a failure rate with a minimum sample, read from one configuration value. The rate is by input fate, the inputs set aside as `Problematic` over those that have settled, so that a failure `HandleFailedInput` retries into a success is not counted and an input still in flight counts neither way.

```python
class Accept:
    ...


def scouting_to_approving(
    *,
    transformations: list[Transformation],
    config: Config,
) -> Accept | dict[UUID, Operation]:
    ...
```

#### The workgraph's `Active` hook

Runs periodically while the workgraph is `Active` and governs the feeders of its members: it proposes new feeder arguments, disables a feeder, or pauses a member. It is the same shape as `ScoutingToApproving`, and it exists because the decision that ends a workgraph is the workgraph's: the target is a property of the request, what has been produced is counted at the end of the chain, and the work in flight spans every member. It receives the members with their input overviews and may take injected dependencies, so it can ask the bookkeeping what has been produced.

```python
def active_hook(
    *,
    workgraph: Workgraph,
    transformations: list[Transformation],
    input_overviews: dict[UUID, dict[Status, int]],
    config: Config,
    bk_db: BookkeepingDB,
) -> dict[UUID, Operation]:
    ...
```

The task that polls a workgraph in `Active` does four things in order:

1. It runs the workgraph's `Active` hook.
2. It applies the operations the hook returned.
3. It runs the `Active` hook of each member in parallel, applying each one's operation as it returns.
4. It moves the workgraph to `Finalizing` if every member is drained: its feeder disabled, and none of its inputs or parcels non-terminal (DX-ADR-005).

Two hooks must not fight over one feeder. The order above means the workgraph's operation on a member is applied before the member's own hook runs, and a member whose feeder is governed by the workgraph's hook should not bind its own.

The simulation workgraph's hook is the worked case. It keeps the events it has asked for, issued seeds times the events per seed, within the target, raising MCSimulation's feeder by a step whenever the work in flight falls below a floor rather than requesting a billion events at once, and it disables the feeder once the events produced, counted from the finished output, reach the target. Undershoot repairs itself: if inputs end `Problematic` and the count falls short, the hook raises the feeder again, which is safe because the cascade below cannot start until the feeder is explicitly disabled. Overshoot is bounded rather than eliminated: whatever is in flight when the feeder is disabled still completes, and the hook lowers its in-flight ceiling as the target approaches so that the excess stays within a step. For a filtered simulation the events produced are those after the filter, which the hook reads from the bookkeeping, and the events per seed are the retention measured during scouting, which is what the extended scout of a filtered workgraph exists to establish. A data workgraph's hook is simpler: it moves `EndRun` forward on the staging and processing feeders as data-taking continues, and the feeders exhaust themselves once the closed range is fully registered.

Pausing a member whose failure rate has become excessive is proposed here as well, since the hook already sees every member's overview. Whether it may also request a flush is open.

#### The transformation's `Active` hook

Runs periodically while the transformation is `Active` and returns one operation on itself: new feeder arguments, the feeder disabled, or a pause. It is the transformation-level counterpart of the workgraph's `Active` hook, for a standalone transformation that has no workgraph to carry one, and for a rule that concerns one member alone. Either way, throughput is throttled here, since how much work exists is a question of feeder arguments rather than of how often anything runs.

```python
def example_active_hook(
    *,
    transformation: Transformation,
    input_overview: dict[Status, int],
    config: Config,
) -> Operation | None:
    ...
```

#### `HandleFailedInput`

Runs for each input in `Failed` and decides its fate: back to `Unassigned` to be packed again, `Problematic` for operator attention, or `Split` into finer-grained children. The default retries until `ErrorCount` reaches a configured maximum and then gives up. LHCb's version sends inputs whose parcel failed for a known grid reason back to `Unassigned` and everything else to `Problematic`.

```python
def example_handle_failed_input(
    *,
    input: Input,
    parcel: Parcel,
    config: Config,
) -> Decision:
    ...
```

There is no hook for a failed parcel. What became of each input of a parcel is read from the job's **status report**, an output the CWL document marks for the purpose (DX-ADR-007). The core moves an input reported `SUCCESS` to `Processed`, and an input reported `FAILURE` to `Failed` with its `ErrorCount` incremented, which is where this hook sees it. For an input reported `SPLIT` the core inserts the portions the report lists as succeeded as `Processed` children claimed by a born-`Done` recovery parcel, and the rest as `Unassigned` children (DX-ADR-005). The children are written in the report in the shape the feeder yields, `Input` or `FileInput`, the same shape `SplitInto` uses.

### Actions

An action is one entry of an ordered list bound to a state (see the Common rules). Every action has the same signature, receives its own arguments from its row in `Actions` (DX-ADR-004), and returns an `ActionResult`:

```python
class ActionResult:
    result: Literal["Passed", "Failed", "Pending", "Done"]
    message: str = ""
    # per transformation, applied by the core when the action passes
    updated_requirements: dict[UUID, Requirements] = {}


def check_success_rate(
    *,
    workgraph: Workgraph,
    transformations: list[Transformation],
    args: CheckSuccessRateArgs,
    config: Config,
) -> ActionResult:
    ...
```

#### `Approving` actions

Run in order while the workgraph is `Approving`. Each action is a check on the scout, and can hand back updated requirements for members: a resource-usage estimate that fails if the usage is too high and otherwise writes the measured needs into the members' requirements templates. An action that needs a person, such as sign-off by the production manager, returns `Failed` until the sign-off has been given, and giving it is forcing the action to passed; `Pending` is for an action still at work, not for one waiting on a person.

Which members start when the workgraph moves to `Active` is declared in the CWL document (DX-ADR-007), not decided by an action.

#### `Finalizing`, `Archiving` and `Cleaning` actions

Run in order while the transformation is `Finalizing`, `Archiving` or `Cancelling`, with the same action signature as the workgraph's minus the list of members. Within a workgraph the members run each of these lists upstream first: a member's list starts once every member it depends on has finished its own, and members with no dependency between them run theirs at the same time (DX-ADR-005). Finalizing actions check and complete the deliverable: no input processed twice, every input the feeder should have produced present, the quarantined inputs reviewed, the output files present in the catalogue, the monitoring histograms merged. Archiving actions remove intermediates and finally the transformation's bulk rows; cleaning actions remove the outputs as well. The database cleanup is itself an action, the last in both lists, so an installation can put its own actions before it.

`CancelInFlightParcels` is the archetypal `Pending` action: its first run asks the backends to cancel and returns `Pending`, and it keeps returning `Pending` until every parcel is terminal (DX-ADR-005).

### Bindings

The hook bindings live in the entity's `Hooks` column and the action lists are rows in `Actions`, ordered by position (DX-ADR-004). Both are resolved from the configuration service against the workgraph's `type` at submission, and a document naming its own overrides that resolution for the entity it is written on (DX-ADR-007). For a workgraph:

```json
{
  "ScoutingToApproving": {"hook": "LHCbCompleteSimulation", "args": {}},
  "Active": {"hook": "LHCbMCTarget", "args": {"target_events": 1000000000, "max_events_in_flight": 10000000}}
}
```

| Position | Action                  | Args                                            | Result    |
| -------- | ----------------------- | ----------------------------------------------- | --------- |
| 1        | `CheckSuccessRate`      | `{"min_success_rate": 0.9, "min_passed": 80}`   | `Passed`  |
| 2        | `EstimateResourceUsage` | `{"max_cpu_hours": 1000, "max_memory_gb": 500}` | `Pending` |
| 3        | `ManualApproval`        | `{"role": "mc_production_manager"}`             | `NULL`    |

For a transformation, with one action list per state:

```json
{
  "HandleFailedInput": {"hook": "RetryUntilMaxErrors", "args": {"max_errors": 10}}
}
```

| State        | Position | Action                     |
| ------------ | -------- | -------------------------- |
| `Finalizing` | 1        | `ReconcileFeederInputs`    |
| `Finalizing` | 2        | `CheckOutputFiles`         |
| `Finalizing` | 3        | `LHCbDoublyProcessedCheck` |
| `Archiving`  | 1        | `CleanIntermediates`       |
| `Archiving`  | 2        | `CleanDatabaseEntries`     |
| `Cancelling` | 1        | `CancelInFlightParcels`    |
| `Cancelling` | 2        | `RemoveOutputFiles`        |
| `Cancelling` | 3        | `RemoveIntermediateFiles`  |
| `Cancelling` | 4        | `CleanDatabaseEntries`     |

### Backends

Backends are not an extension point. The set is fixed by [DX-ADR-003](DX-ADR-003_compute_backends.md).

## Rationale

- **Plugins propose, the core writes.** Because only the core opens the transaction that transitions state and journals counters, no plugin, however buggy, can leave the state machine or the counters inconsistent. It is the plugin-surface version of DX-ADR-001's move away from coordinating components through shared mutable state.
- **In-state hooks with persisted results.** Running plugin code inside a transition would make the transition slow and able to fail half-way. Running it inside a state, with each action's result written on its own row, makes a list of actions resumable, lets an operator see exactly which check failed, and keeps every transition a small transaction (DX-ADR-005).
- **A bookmark instead of a constraint.** The feeder is the only writer of new inputs, so the cheapest place to stop duplicates is the feeder itself. Storing the bookmark in the transformation row rather than in the plugin's own cache means it commits with the inputs it describes, survives a restart, and is visible to an operator debugging why a feeder has stopped producing. Allowing it to be approximate is what keeps a catalogue query cheap; the reconciliation action is the price.
- **Feeder and packer stay separate.** They change independently and consult different external systems; collapsing them would couple metadata-catalogue knowledge and packing policy into one plugin that every VO must reimplement wholesale.
- **Streaming packers.** The generator contract keeps memory bounded for million-input transformations and lets the core commit in batches, so a large packing run is naturally chunked and restartable.
- **Throttling belongs in the arguments, not the schedule.** How much work a workgraph generates is set by the feeder's arguments and by what the backends accept, and the active hook adjusts the first as the workgraph runs. Making the sweep interval the throttle instead would tie the rate to the deployment rather than to the workgraph, and would break as soon as two installations ran the same workgraph.
- **A status report instead of a parcel hook.** The per-input outcome of a parcel is written by the job itself, as an output in a schema the core knows (DX-ADR-007), so the core reads it directly and the only failure hook is the one that reads an input's history and knows how often it has failed. The transitions it proposes are defined in DX-ADR-005.

## Rejected Ideas

- **Plugins that write entity tables directly.** Maximum flexibility, but every plugin becomes a place state and counters can drift; rejected in favour of propose/write.
- **A single "transformation plugin" covering feed + pack.** Fewer entry points, but forces VOs to reimplement both to change one, and couples unrelated concerns.
- **Hooks on transitions.** An earlier draft bound hooks to transitions (`MoveToActive`, `MoveToValidating`) and let their verdicts decide them. See the Rationale and DX-ADR-005; the same decisions are now taken inside states, and "the transformation is finished" is a condition the core checks rather than a hook's verdict.
- **Feeder bookmarks in an external cache.** An earlier draft of the bookkeeping example kept its watermark in Redis. It cannot commit with the inputs, so a crash between the insert and the cache write loses or duplicates work, and an operator cannot see it.
- **One sweep for every transformation's feeders.** A single periodic task holding one mutex would serialise thousands of feeders behind the slowest catalogue query; the sweep enqueues per-transformation tasks instead.
- **A first-class reset/failure plugin.** An earlier draft made failure policy its own pluggable role; it is one hook on the input state machine, plus the status report, instead.
- **A `HandleFailedParcels` hook.** An earlier draft had a hook that ran for each `Failed` or `PartiallyDone` parcel, read the backend's report, and said which portions of each input had succeeded. The job writes that itself as its status report (DX-ADR-007), and the hook is gone.
- **Splitting as a packer responsibility.** Conflates failure and recovery policy with packing; splits come from the status report or from the `HandleFailedInput` hook.
- **Monitoring inside the periodic hooks.** They already run on every sweep with the entity's state loaded, so publishing a number from one looks free. See the common rules: the mutex, the failure mode and the coverage all make it the wrong place, and the counters of DX-ADR-009 serve any reader at any cadence.
- **Hooks as schema structure** (edge tables, trigger rows). Cross-transformation behaviour is open-ended orchestration; expressing it as data-driven plugins keeps the schema free of one-off relationships.

## Open Issues

- **Input identity for external feeders.** The bookmark stops a feeder re-yielding, but nothing recognises the same input arriving from two different feeders, or the same file appearing under two LFNs. Whether the core needs a notion of input identity at all, beyond the reconciliation action, is open. Internal edges reconcile against the producer's `ParcelOutputs` rows by LFN.
- **Flushes from the workgraph's hooks.** A transformation's own `Active` hook requests its periodic flush, since the cadence follows what feeds that transformation. Whether the workgraph's `Active` hook or `ScoutingToApproving` may also request one, which the LHCb simulation flow wants at the end of scouting, is open.
- **Parcels without a usable status report.** What the core does with a parcel whose job wrote no report, or whose report names inputs the parcel did not carry, is open, as is whether an input reported `NOT_TRIED` returns to `Unassigned` directly or passes through `HandleFailedInput`.
- **Reconciliation cost.** A full re-evaluation of a catalogue query at `Finalizing` is what makes an approximate bookmark safe, and it has not been measured against a realistic query (see `schema-benchmarks.md`).
- **Discovery and versioning.** How entry-point-discovered plugins are versioned and pinned per transformation, so that a plugin upgrade cannot silently change a running transformation's behaviour, is open.
