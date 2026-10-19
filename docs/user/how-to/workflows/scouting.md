# Scouting simulation

Run the simulation workgraph on a small sample first, measure what it needs, and have a person sign it off before it runs at full scale. This extends [Basic simulation](simulation.md).

## The workgraph

```workgraph
{
  name: "simulation",
  workgraph: { scouting: { stages: [8, 16], failAbove: 0.05, minSeen: 10 }, approving: [{ name: "check success rate", check: "success rate" }, { name: "estimate resource usage", fail: "once" }, { name: "manual approval", manual: true }], target: { output: "datasets", files: 20 } },
  transformations: {
    simulation: { label: "MCSimulation", feeder: { seeds: 600, batch: 60 }, packer: { size: 1 }, run: [1.4, 2.6], finalize: ["no seed used twice"] },
    reco: { label: "MCReconstruction", feeder: { from: "simulation" }, packer: { size: 3 }, finalize: ["no input used twice"] },
    merge: { label: "MCMerge", feeder: { from: "reco" }, packer: { size: 4 }, finalize: ["no input used twice"] },
  },
  outputs: { datasets: { from: "merge", label: "datasets" } },
}
```

The same chain as the basic simulation, with a scouting ladder of eight then sixteen seeds and a list of approving actions on the workgraph, which the hook accepts early once more than one in twenty of at least ten settled inputs have ended `Problematic`. The resource estimate is set to fail once so that the model shows a blocked approval. Turn **chaos** on during the scout to see the other way a scout ends: the hook accepts early and the success-rate check blocks the workgraph.

## The document

!!! info "CWL document (placeholder)"

    The document form is pending the `dirac:` hint schema of DX-ADR-007. This pattern needs:

    - a workflow input carrying a `dirac:Feeder` hint naming the seed feeder, with the requested number of events and the scouting sample as arguments;
    - three steps, each with a `dirac:Transformation` hint naming its packer; each runs during the scout because it is a compute step, which the document does not have to say;
    - the approving actions and the sign-off in the `dirac:Workgraph` hint, or the VO defaults that supply them, which is also what makes the workgraph scout at all;
    - the ladder in the `dirac:Workgraph` hint's `ScoutingToApproving` binding, stated in the feeder's own unit;
    - the `dirac:Workgraph` hint with the schema version and the VO type that supplies the defaults;
    - the merge output declared as the workflow output.

## What happens

1. Submitting the document creates the workgraph and its three transformations in `New`. Starting it moves the workgraph to `Scouting` and the transformations to `Active`.
2. In scouting mode the seed feeder yields only the current stage of the sample. The parcels run, the reconstruction and the merge pick up the outputs along the edges, and when every member has drained the `ScoutingToApproving` hook raises the feeder to the next stage; after the last it accepts and the workgraph moves to `Approving`. The line under the state strip says which stage the scout is at and how many inputs have been processed or set aside. A failed parcel is not yet a failure: `HandleFailedInput` retries its inputs, and only an input that exhausts its retries is set aside as `Problematic`. Should more than one in twenty of at least ten settled inputs end that way, the hook accepts at once instead, knowing the success-rate check will fail: a scout in trouble shows as `ApprovingBlocked`, and a scout still running is plainly `Scouting`, so nobody has to watch it.
3. The approving actions run in order: the success rate is checked against the same threshold the hook used, the resource usage is measured and written into the requirements, and the workgraph waits for sign-off. In the model the resource estimate fails the first time, which moves the workgraph to `ApprovingBlocked`; **reset** it or **force** it **passed**. The sign-off blocks the same way until someone gives it: **force passed** is the sign-off. **scout further** adds a stage to the ladder and returns to `Scouting`, keeping everything the scout produced.
4. In `Active` the workgraph's active hook governs MCSimulation's feeder: it raises the limit in steps so that only a bounded number of events is in flight, and it will disable the feeder once the dataset holds the requested number of files. Watch the feeder strip climb in steps of 60 as the hook raises the limit each time the work in flight drops. Nothing the scout produced is redone. The line under the strip reads `requesting` while an external feeder is enabled and `feeders done, draining` once none is: draining is a condition inside `Active`, not a state of its own.
5. When the requested amount has been produced, here 20 files in the dataset, the hook disables the feeder. Parcels already in flight still complete, so the workgraph ends a little over the target. The chain then drains from the top: each packer, told that its feeder is off, packs what is left, and each downstream feeder reports exhaustion once its producer is finished.
6. The workgraph moves to `Finalizing` and runs each member's finalizing checks upstream first, then reaches `Completed`.

## Finalizing checks

**MCSimulation**
:   No seed was used twice.

**MCReconstruction**
:   No input was used twice.

**MCMerge**
:   No input was used twice.

## See also

- [Basic simulation](simulation.md) is the same workgraph without the scouting phase.
- [Replicating the output](replication.md) adds a replication that must not run during scouting.
- [How the Transformation System works](../../explanations/transformation-system.md#the-workgraph-state-machine) explains scouting and approval.
- The [worked examples](../../../adr/examples.md) trace the LHCb simulation in full, including the filtered variant.
