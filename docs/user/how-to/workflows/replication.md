# Replicating the output

Replicate the workgraph's deliverable to its destinations as it is produced, without replicating anything during scouting. This extends [Scouting simulation](scouting.md).

## The workgraph

```workgraph
{
  name: "simulation + replication",
  workgraph: { scouting: { count: 15 } },
  transformations: {
    simulation: { label: "MCSimulation", feeder: { seeds: 200 }, packer: { size: 1 }, run: [1.4, 2.6] },
    reco: { label: "MCReconstruction", feeder: { from: "simulation" }, packer: { size: 3 } },
    merge: { label: "MCMerge", feeder: { from: "reco" }, packer: { size: 4 } },
    replication: { kind: "replication", label: "output replication", feeder: { from: "merge" }, packer: { size: 2 }, hold: "approval", finalize: ["every input replicated or absent"] },
  },
  outputs: { datasets: { from: "merge", label: "datasets" } },
}
```

The replication is a data transformation fed by the merge's output. It is held in `Paused` while the workgraph scouts and starts when the workgraph is approved, so the scout's output is only replicated once the workgraph is going ahead.

## The document

!!! info "CWL document (placeholder)"

    The document form is pending the `dirac:` hint schema of DX-ADR-007. This pattern adds to the simulation's document:

    - a data-management declaration in the `dirac:Workgraph` hint against the merge step's output, naming the replication packer and the destinations, or the VO type whose defaults supply them;
    - its initial state, held back until approval;
    - the finalizing check bound to it.

## What happens

1. The replication is created in `New` with the rest and moves to `Paused` when the workgraph starts scouting.
2. Merge outputs produced during the scout are recorded like any other, but with the replication `Paused` its feeder does not run, so they wait.
3. Approval moves the workgraph to `Active` and the replication with it. Its edge feeder picks up everything recorded so far and everything that follows.
4. Each replication parcel is a request that copies its files to the destinations. The counters of a data transformation are kept per destination and in bytes as well as files, which is what its operators watch.
5. The replication is the last member to drain, since its feeder reports exhaustion only once the merge is finished, and its finalizing check runs last.

## Finalizing checks

**output replication**
:   Every input is `Processed`, or is `NotProcessed` and absent from the file catalogue.

## Variations

- A replication can be declared against an intermediate output as well as the deliverable, for example to stage the reconstruction output somewhere before the merge.
- A workgraph whose scouting is skipped starts its replication with the rest.

## See also

- [Processing data from a query](data-processing.md) uses a replication for staging before the first step rather than after the last.
