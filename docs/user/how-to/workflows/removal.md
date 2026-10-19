# Removing intermediates

Remove the files a step produced once the step that consumes them has processed them, using removal transformations rather than in-job deletion. This extends [Scouting simulation](scouting.md).

## The workgraph

```workgraph
{
  name: "simulation + removal",
  workgraph: { scouting: { count: 15 } },
  transformations: {
    simulation: { label: "MCSimulation", feeder: { seeds: 200 }, packer: { size: 1 }, run: [1.4, 2.6] },
    reco: { label: "MCReconstruction", feeder: { from: "simulation" }, packer: { size: 3 } },
    merge: { label: "MCMerge", feeder: { from: "reco" }, packer: { size: 4 } },
    removalSim: { kind: "removal", label: "MCSimulationRemoval", feeder: { from: "simulation", after: ["reco"] }, packer: { size: 4 }, finalize: ["only NotProcessed files remain", "removed files flagged in the bookkeeping"] },
    removalReco: { kind: "removal", label: "MCReconstructionRemoval", feeder: { from: "reco", after: ["merge"] }, packer: { size: 4 }, finalize: ["only NotProcessed files remain", "removed files flagged in the bookkeeping"] },
  },
  outputs: { datasets: { from: "merge", label: "datasets" } },
}
```

Two data transformations join the chain. Each is fed by the same output as the step it shadows, and its packer takes a file only once that step has processed it. The alternative, the consuming job deleting its inputs after registering its own outputs, needs no transformation but leaves nothing to check at the end.

## The document

!!! info "CWL document (placeholder)"

    The document form is pending the `dirac:` hint schema of DX-ADR-007. This pattern adds to the simulation's document:

    - two data-management declarations in the `dirac:Workgraph` hint, one against the simulation step's output and one against the reconstruction step's output, each naming the removal packer;
    - the initial state of each removal, since whether they run during scouting is a choice;
    - the finalizing checks bound to each removal.

## What happens

1. The removals are created with the workgraph. Whether they start with the compute steps or wait for approval is declared in the document; either way they are started at the latest when the workgraph becomes `Active`.
2. When a simulation parcel reaches `Done`, its output files are recorded under the simulation's declared output. The removal's edge feeder reads them like any other consumer, with its own bookmark.
3. The removal's packer delays each file until the reconstruction's input for that file is `Processed`, which a lookup of the LFN in the reconstruction's inputs answers. The delayed inputs sit faded in the pool.
4. A removal parcel is a request to the data-management system rather than a job. When it completes, its inputs move to `Processed` and the replicas are gone.
5. A file the reconstruction wrote off as `NotProcessed` is never removed, so that it can be looked at.
6. The removals drain with the rest of the chain and run their finalizing checks in DAG order after their producers.

## Finalizing checks

**MCSimulationRemoval**
:   The simulation output directory contains only files that the reconstruction left `NotProcessed`.
:   Every removed simulation output is flagged as having no replica in the bookkeeping.

**MCReconstructionRemoval**
:   The reconstruction output directory contains only files that the merge left `NotProcessed`.
:   Every removed reconstruction output is flagged as having no replica in the bookkeeping.

## See also

- [Splitting an output](splitting.md): a removal whose file has two consumers waits for both.
- [Processing data from a query](data-processing.md) removes staged buffer copies, which are workgraph inputs rather than step outputs.
