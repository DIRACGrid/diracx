# Basic simulation

Produce a requested number of simulated events, reconstruct them, and merge the reconstructed files into the deliverable.

## The workgraph

```workgraph
{
  name: "simulation",
  workgraph: { target: { output: "datasets", files: 20 } },
  transformations: {
    simulation: { label: "MCSimulation", feeder: { seeds: 600, batch: 60 }, packer: { size: 1 }, run: [1.4, 2.6], finalize: ["no seed used twice"] },
    reco: { label: "MCReconstruction", feeder: { from: "simulation" }, packer: { size: 3 }, finalize: ["no input used twice"] },
    merge: { label: "MCMerge", feeder: { from: "reco" }, packer: { size: 4 }, finalize: ["no input used twice"] },
  },
  outputs: { datasets: { from: "merge", label: "datasets" } },
}
```

Three compute transformations in a chain. The simulation has no input files: its feeder issues seeds, one per parcel. The reconstruction is fed by the simulation's outputs as they appear and groups three files per parcel; the merge groups four reconstructed files into one and delivers the dataset.

## The document

!!! info "CWL document (placeholder)"

    The document form is pending the `dirac:` hint schema of DX-ADR-007. This pattern needs:

    - a workflow input carrying a `dirac:Feeder` hint naming the seed feeder, with the requested number of events as its argument;
    - three steps, each with a `dirac:Transformation` hint naming its packer (one seed per parcel; by size for the reconstruction and the merge);
    - the `dirac:Workgraph` hint with the schema version and the VO type that supplies the defaults;
    - the merge output declared as the workflow output.

## What happens

1. Submitting the document creates the workgraph and its three transformations in `New`. Starting it moves the workgraph and the transformations to `Active`.
2. The workgraph's active hook governs MCSimulation's feeder: it raises the limit in steps so that only a bounded number of events is in flight, and it will disable the feeder once the dataset holds the requested number of files. Watch the feeder strip climb in steps of 60 as the hook raises the limit each time the work in flight drops.
3. The reconstruction and the merge pick up the outputs along the edges as they appear. Each packer waits for a full group while its feeder is active.
4. When the requested amount has been produced, here 20 files in the dataset, the hook disables the feeder. Parcels already in flight still complete, so the workgraph ends a little over the target. The chain then drains from the top: each packer, told that its feeder is off, packs what is left, and each downstream feeder reports exhaustion once its producer is finished.
5. The workgraph moves to `Finalizing` and runs each member's finalizing checks upstream first, then reaches `Completed`.

## Finalizing checks

**MCSimulation**
:   No seed was used twice.

**MCReconstruction**
:   No input was used twice.

**MCMerge**
:   No input was used twice.

## See also

- [Scouting simulation](scouting.md) runs the same workgraph on a sample first and waits for sign-off.
- [Removing intermediates](removal.md) removes the simulation and reconstruction outputs once consumed.
- [Replicating the output](replication.md) adds the replication of the merged dataset.
- [Collecting histograms](artifacts.md) handles the monitoring histograms each job produces.
- The [worked examples](../../../adr/examples.md) trace the LHCb simulation in full, including the filtered variant.
