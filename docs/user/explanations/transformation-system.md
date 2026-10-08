# How the Transformation System works

The Transformation System runs **workgraphs**: a set of transformations, each applying one operation to a pool of inputs and handing what it produces to the next. This page explains the pieces and how they connect: what a transformation does, the statuses an input and a parcel go through, what the feeder and the packer each decide, how one transformation feeds the next, what happens when jobs fail, and how the workgraph's own state machine drives its members. The vocabulary is that of [DX-ADR-002](../../adr/DX-ADR-002_overview.md), and the [how-to guides](../how-to/workflows/index.md) show how to express particular workflows.

!!! info "The models are interactive"

    The models on this page are running simulations of the system. Each waits in `New` until you pick a speed, or press pause to start it held. The toolbar pauses a model, steps it on to the next thing that happens, sets its speed or runs it again, expert for the rails that run the feeders, the packers and the workgraph's hooks by hand, and chaos for a fifty percent failure rate; a line under the title says what the workgraph is doing or waiting on and opens its checks and actions, the `?` explains the picture, clicking a file shows where it came from, and a model holds still while one of its dialogs is open or while it is scrolled out of view.

## Reading the pictures

```workgraph
{
  name: "reading the pictures",
  sources: { query: { files: 120 } },
  transformations: {
    reco: { feeder: { from: "query" }, packer: { size: 2 } },
  },
  outputs: { datasets: { from: "reco", label: "datasets" } },
}
```

**Card**
:   A **transformation**. The well on the left is its **input pool**, and the grid of slots on the right holds its **parcels**: bundles of inputs handed over to run as jobs, one slot per job the backend runs at once. A slot fills like a container while its job runs, a dashed border means it is waiting on the backend, and a halo in the outcome's colour marks one that ended a moment ago; the chip on the last slot counts the parcels waiting for one. While a list of actions runs, the running action takes the pool's place. In expert mode a rail down the card's right edge carries a row per sweep, each automatic or by hand, with a ▶ on a row that is by hand and has something to do.

**Boxes above and below**
:   The box above the workgraph is the **input query**: the files the experiment's metadata catalogue returns for it. The boxes below are what the workgraph delivers.

**Colour and shape**
:   Colour says which source file an item descends from, all the way down the graph. Shape says the file type.

**Counts**
:   The bar along the card's bottom edge is the feeder: the inputs by state against its limit, green `Processed`, blue `Assigned`, red `Failed` or `Problematic`, amber `Split`, grey `Unassigned`. A click anywhere on the card opens its dialog on the transformation's status: the inputs and the parcels by state on the left, the feeder, the packer, the hooks and every action list with its results on the right, with the transformation's own state machine and those of its inputs and its parcels behind three more tabs. A feeder fed by an edge has no limit, since its producer is still producing, so its bar is the states of what it has fed so far. A `+N` chip above a card's input port counts files made upstream that its feeder has not swept yet; the chip on the slots counts parcels waiting for a slot, and clicking it shows the parcel counts by status.

## Transformations and the input pool

A transformation does three things in a loop. Its **feeder** evaluates the input query and adds the files it finds to the pool. Its **packer** groups pooled inputs into parcels. Each parcel runs as a job, and when the job finishes its outputs are registered in the catalogue and appear in the dataset.

Every unit of work a transformation might process is an **input**: usually a file, sometimes a fraction of one, sometimes a seed. Inputs start `Unassigned` in the pool. The packer moves the ones it takes to `Assigned`; a parcel that finishes moves them to `Processed`; one that fails moves them to `Failed`, where a handler decides between another attempt, a `Split`, and quarantine in `Problematic`. `Processed`, `Split` and `NotProcessed` are terminal. `Problematic` is a quarantine: it waits for an operator, and holds its transformation open until someone resets it or writes it off. An input is successfully processed by exactly one parcel.

Every model shows the workgraph's states under its title, with the current one lit, and the event log under the picture.

```workgraph
{
  name: "the input pool",
  sources: { query: { files: 160 } },
  transformations: {
    reco: { feeder: { from: "query" }, packer: { size: 2 }, fail: 0.15, retries: 1 },
  },
  outputs: { datasets: { from: "reco", label: "datasets" } },
}
```

## Feeders and their bookmarks

The feeder is the bridge to whatever produces inputs. A catalogue feeder evaluates a metadata query; a simulation feeder has no input files and issues seeds until the requested number of events is covered. Each keeps a bookmark in the transformation row, committed in the same transaction as the inputs it yielded, so that it never yields the same input twice. The dialog's feeder block shows the bookmark as what it has `fed` against the query's total for a catalogue feeder, and as the `seeds` issued against its limit for a seed feeder. Only one feeder invocation runs per transformation at a time.

```workgraph
{
  name: "feeders",
  sources: { query: { files: 120 } },
  transformations: {
    simulation: { feeder: { seeds: 120 }, packer: { size: 1 }, run: [1.4, 2.6] },
    reco: { feeder: { from: "query" }, packer: { size: 2 } },
  },
}
```

## Packers and parcels

The packer decides when and how `Unassigned` inputs become parcels, and nothing else. It sees the pool and whether its feeder is still running, so it can hold out for full groups while input keeps coming and pack the remainder once the feeder is off; the last parcels of the merge below are smaller than the rest for that reason.

A parcel is immutable and is never retried. It is created `Unassigned`, claimed by the dispatcher into `Reserved`, the crash-safe step before the backend acknowledges, runs as `Assigned`, and passes through `Completing` while its outputs are registered and its outcome is worked out. A slot's border carries the parcel's state and its dashes say it is waiting on the backend, so `Reserved` is grey and dashed, `Assigned` blue with the fill following the job, and `Completing` teal and dashed. Every terminal state is final: the inputs of a failed parcel return to the pool if the input handler retries them, and a later parcel may group them differently.

```workgraph
{
  name: "packers and parcels",
  transformations: {
    simulation: { feeder: { seeds: 60 }, packer: { size: 1 }, run: [1.2, 2.2] },
    merge: { feeder: { from: "simulation" }, packer: { size: 4 } },
  },
  outputs: { datasets: { from: "merge", label: "datasets" } },
}
```

## Edges

The workgraph's CWL document declares which step consumes which output. When a compute parcel reaches `Done`, the files it produced are recorded under the output they belong to, and the **edge feeder** of each consumer picks them up as they appear: there is no waiting for the whole upstream stage to finish and no catalogue query to write for an internal edge. The producer does not know who reads its output; each consumer keeps its own bookmark over the recorded files, like any other feeder, so a fan-out costs nothing and a consumer can be added later. A consumer's feeder reports exhaustion once the producer's own feeder is off, none of the producer's inputs or parcels is non-terminal and its bookmark has reached the last recorded file, which is how the end of a workgraph propagates down a chain one transformation at a time. Watch the log say `feeder reported exhaustion, disabled` down the chain, one transformation at a time.

```workgraph
{
  name: "edges",
  transformations: {
    simulation: { feeder: { seeds: 80 }, packer: { size: 1 }, run: [1.2, 2.2] },
    reco: { feeder: { from: "simulation" }, packer: { size: 3 } },
    merge: { feeder: { from: "reco" }, packer: { size: 4 } },
  },
  outputs: { datasets: { from: "merge", label: "datasets" } },
}
```

An output can be consumed by several steps, and a step can take only one type of file. Here a filter emits one output per input and three merges each take one type. A step can also consume two outputs at once, for example to compare two reconstructions of the same files; that is a join, and the [joining guide](../how-to/workflows/joining.md) shows how it is expressed.

```workgraph
{
  name: "fan-out",
  sources: { query: { files: 300 } },
  transformations: {
    reco1: { feeder: { from: "query" }, packer: { size: 2 }, emit: "input" },
    filter1: { feeder: { from: "reco1" }, packer: { size: 3 }, emit: "input" },
    merge1: { label: "merge", feeder: { from: "filter1", type: "circle" }, packer: { size: 3 } },
    merge2: { label: "merge", feeder: { from: "filter1", type: "triangle" }, packer: { size: 3 } },
    merge3: { label: "merge", feeder: { from: "filter1", type: "diamond" }, packer: { size: 3 } },
  },
  outputs: {
    t1: { from: "merge1", label: "type 1", show: "histogram" },
    t2: { from: "merge2", label: "type 2", show: "histogram" },
    t3: { from: "merge3", label: "type 3", show: "histogram" },
  },
}
```

## Data transformations and delayed inputs

A **data transformation** makes or removes replicas rather than running jobs: its parcels are requests to the data-management system. It records no outputs, so a step that needs a file staged first shares the staging's input query, and its packer delays each file until the buffer copy exists. Delay is not a status: the input stays `Unassigned`, withheld from the packer, drawn faded in the pool and counted as delayed beside `Unassigned` in the dialog. A removal declared against the workgraph's inputs works the same way in reverse: its packer takes a file only once the consuming step has processed it. Here the removal is held `Paused` until an operator starts it from the button in the toolbar.

```workgraph
{
  name: "staging and removal",
  sources: { query: { files: 120 } },
  transformations: {
    staging: { kind: "replication", feeder: { from: "query" }, packer: { size: 2 } },
    spruce: { label: "sprucing", feeder: { from: "query", after: ["staging"] }, packer: { size: 2 } },
    removal: { kind: "removal", label: "buffer removal", feeder: { from: "query", after: ["spruce"] }, packer: { size: 4 }, hold: "operator" },
  },
  outputs: { datasets: { from: "spruce", label: "datasets" } },
}
```

## Failure handling

A job that fails returns its inputs to the pool, marked with the red `retry` loop, and a later parcel picks them up again. The job's own status report says what became of each input, so there is no parcel-level handler. The input handler then decides each input's fate: back to `Unassigned` for another attempt, or `Problematic` once it has failed too often, counted under `Pb` and shown as `✗` to the operator.

A partial outcome, the CMS case of a job that processed some luminosity sections of a file and not others, becomes a **split**: the parent input goes to `Split`, the processed portion is inserted as a `Processed` child claimed by a recovery parcel born `Done` (a slot that appears already finished), and the unprocessed portion is inserted as an `Unassigned` child, drawn smaller, to be packed again. Children keep their parent's file and lineage.

```workgraph
{
  name: "failure handling",
  sources: { query: { files: 160 } },
  transformations: {
    process: { feeder: { from: "query" }, packer: { size: 2 }, fail: 0.1, partial: 0.3, retries: 1, emit: "input" },
    merge: { feeder: { from: "process" }, packer: { size: 4 } },
  },
  outputs: { datasets: { from: "merge", label: "datasets" } },
}
```

## The workgraph state machine

The workgraph's status rolls up its members and is the control surface. A workgraph can start with a **scouting** phase, in which the transformations run on a small sample to show that the configuration works and to measure what it needs. A workgraph with something to scout and an approving list to judge it starts in `Scouting`: the members that run during the scout go `Active`, the rest are held `Paused`, and the feeders yield only the scouting sample. When every active member has drained, the workgraph moves to `Approving` and runs its approving actions; here the sign-off waits for a person, so the workgraph blocks until you **force** it **passed**, which is the sign-off. Approval moves it to `Active`, which starts the held members and lifts the feeders' limits. Nothing the scout produced is redone.

The workgraph leaves `Active` only when every member is drained: its feeder is off, and it has no non-terminal inputs or parcels. It then moves to `Finalizing` and finalises its members upstream first, running each one's finalizing actions in order, and reaches `Completed` when the last member is finalised. After a delay it moves to `Archiving`, where each member runs its archiving actions and its bulk rows are deleted while its outputs stay. Cancelling a workgraph runs the cleaning actions instead, which remove the outputs as well. While a list runs, the running action takes the pool's place on the card, and the panel above the picture shows every list that is running with the result of each action as it lands. The strip at the top of the panel follows these transitions. Here the resource estimate fails the first time, so the workgraph shows `ApprovingBlocked` until you rerun the action or force it.

```workgraph
{
  name: "state machine",
  workgraph: { scouting: { count: 10 }, approving: [{ name: "check success rate", check: "success rate" }, { name: "estimate resource usage", fail: "once" }, { name: "manual approval", manual: true }] },
  transformations: {
    simulation: { feeder: { seeds: 120, batch: 40 }, packer: { size: 1 }, run: [1.2, 2.2], finalize: ["no seed used twice"] },
    reco: { feeder: { from: "simulation" }, packer: { size: 3 }, finalize: ["no input used twice"] },
    replication: { kind: "replication", feeder: { from: "reco" }, packer: { size: 3 }, hold: "approval" },
  },
  outputs: { datasets: { from: "reco", label: "datasets" } },
}
```

## Where to read more

- The [how-to guides](../how-to/workflows/index.md) express common workflows in these terms.
- [DX-ADR-002](../../adr/DX-ADR-002_overview.md) fixes the model and the vocabulary.
- [DX-ADR-003](../../adr/DX-ADR-003_compute_backends.md) is the backend contract and the dispatcher.
- [DX-ADR-004](../../adr/DX-ADR-004_schema.md) is the schema, including the parcel outputs and the counters.
- [DX-ADR-005](../../adr/DX-ADR-005_state_machines.md) is the four state machines shown here.
- [DX-ADR-006](../../adr/DX-ADR-006_extensions.md) is the feeder, packer, hook and action contracts.
- [DX-ADR-007](../../adr/DX-ADR-007_cwl.md) is the CWL document a workgraph is written in.
- [Worked examples](../../adr/examples.md) trace the LHCb and CMS workgraphs through the model.
