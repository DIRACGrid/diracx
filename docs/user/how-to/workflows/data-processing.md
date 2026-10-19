# Processing data from a query

Process the files a bookkeeping query returns, extending the run range as data-taking continues, and stage them from tape first when they are not on disk.

## The workgraph

### From a query

```workgraph
{
  name: "from a query",
  workgraph: { scouting: { count: 12 } },
  sources: { query: { files: 200 } },
  transformations: {
    reco: { feeder: { from: "query" }, packer: { size: 2 }, finalize: ["no input used twice", "every file the query returns is present"] },
    merge: { feeder: { from: "reco" }, packer: { size: 3 }, finalize: ["no input used twice"] },
    replication: { kind: "replication", feeder: { from: "merge" }, packer: { size: 2 }, hold: "approval" },
  },
  outputs: { datasets: { from: "merge", label: "datasets" } },
}
```

The plain case: a reconstruction fed by a bookkeeping query, a merge, and a replication of the merged output.

### With staging from tape

```workgraph
{
  name: "with staging",
  workgraph: { scouting: { count: 10 } },
  sources: { query: { files: 200 } },
  transformations: {
    staging: { kind: "replication", label: "staging", feeder: { from: "query" }, packer: { size: 2 } },
    spruce: { label: "sprucing", feeder: { from: "query", after: ["staging"] }, packer: { size: 2 }, finalize: ["no input used twice"] },
    merge: { feeder: { from: "spruce" }, packer: { size: 3 }, finalize: ["no input used twice"] },
    replication: { kind: "replication", feeder: { from: "merge" }, packer: { size: 2 }, hold: "approval", finalize: ["every input replicated or absent"] },
    removal: { kind: "removal", label: "buffer removal", feeder: { from: "query", after: ["spruce"] }, packer: { size: 4 }, hold: "operator" },
  },
  outputs: { datasets: { from: "merge", label: "datasets" } },
}
```

LHCb sprucing: the input files are on tape, so a staging replication copies them to BUFFER storage, the sprucing transformation shares the query and its packer delays each file until its buffer copy exists, and a removal declared against the workgraph's inputs removes the buffer copies once the sprucing has processed them.

## The document

!!! info "CWL document (placeholder)"

    The document form is pending the `dirac:` hint schema of DX-ADR-007. This pattern needs:

    - a workflow input carrying a `dirac:Feeder` hint naming the bookkeeping feeder, with the query, the full run range and the scouting run range as arguments;
    - the steps with their `dirac:Transformation` hints and the workgraph's active hook that moves the end of the run range forward;
    - for staging, a data-management declaration in the `dirac:Workgraph` hint against the workflow input, naming the staging packer and the buffer storage, and a second declaration against the same input for the removal with its initial state held back;
    - the merge output declared as the workflow output and the replication declared against it.

## What happens

1. During scouting the feeder yields only the files in the scouting run range. The compute steps and the staging run; the output replication waits for approval and the removal for the operator.
2. With staging, every file reaches two pools: the staging's, whose parcels copy it to the buffer, and the sprucing's, where it sits delayed until the staging's input for it is `Processed`. A data transformation records no outputs, so the two transformations are connected by the shared query and the delay, not by an edge.
3. After approval the workgraph's active hook moves the end of the run range forward over time on both the staging and the sprucing feeders, towards the requested end, so that the workgraph follows data-taking rather than requesting everything at once.
4. The removal, once started, takes a file only when the sprucing has processed it.
5. When the end of the range has been reached and the feeders have yielded everything, the hook disables them and the chain drains. The reconciliation check at `Finalizing` re-evaluates the query without the bookmark and fails if anything the feeder should have produced is missing.

## Finalizing checks

**reconstruction**
:   No input was used twice.
:   Every file the query returns is present.

**merge**
:   No input was used twice.

**replication**
:   Every input is `Processed`, or is `NotProcessed` and absent from the file catalogue.

## See also

- [Adding ancestor files](ancestry.md) extends the staged case to jobs that need a second file.
- The [worked examples](../../../adr/examples.md) trace sprucing and RDST stripping in full.
