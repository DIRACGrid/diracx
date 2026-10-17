# Expressing workflows

!!! warning "Work in progress"

    The document form is not final: the `dirac:` hint schema of [DX-ADR-007](../../../adr/DX-ADR-007_cwl.md) is still being written, so each guide carries a placeholder listing what its document needs.

These guides show how to express common workflows as workgraphs within the DiracX transformation system. Each starts from a goal, shows the workgraph as a running model, marks where the CWL document goes, and describes what the system does with it at run time, in the terms of [How the Transformation System works](../../explanations/transformation-system.md).

<div class="grid cards" markdown>

- :material-atom:{ .lg .middle } **[Basic simulation](simulation.md)**

    ______________________________________________________________________

    Seeds in, simulated, reconstructed and merged events out.

- :material-magnify-scan:{ .lg .middle } **[Scouting simulation](scouting.md)**

    ______________________________________________________________________

    The same workgraph run on a sample first, with approving actions and a sign-off.

- :material-delete-sweep:{ .lg .middle } **[Removing intermediates](removal.md)**

    ______________________________________________________________________

    Removal transformations for the files a downstream step has consumed.

- :material-content-duplicate:{ .lg .middle } **[Replicating the output](replication.md)**

    ______________________________________________________________________

    A replication of the workgraph's deliverable, started on approval.

- :material-database-search:{ .lg .middle } **[Processing data from a query](data-processing.md)**

    ______________________________________________________________________

    A bookkeeping query as input, run ranges extended over time, and staging from tape.

- :material-file-tree:{ .lg .middle } **[Adding ancestor files](ancestry.md)**

    ______________________________________________________________________

    Jobs that need a second file found by lookup, the RDST and RAW case.

- :material-chart-histogram:{ .lg .middle } **[Collecting histograms](artifacts.md)**

    ______________________________________________________________________

    Small files from every job, merged by a step, by a finalizing action, or both.

- :material-call-split:{ .lg .middle } **[Splitting an output](splitting.md)**

    ______________________________________________________________________

    One output consumed by two steps, and a removal that waits for both.

- :material-call-merge:{ .lg .middle } **[Joining two outputs](joining.md)**

    ______________________________________________________________________

    One step consuming the outputs of two others for the same source file.

</div>
