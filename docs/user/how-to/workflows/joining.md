# Joining two outputs

Consume the outputs of two steps in one job, matched by the source file they descend from, for example to compare two reconstructions of the same data.

## The workgraph

```workgraph
{
  name: "joining",
  sources: { query: { files: 200 } },
  transformations: {
    recoA: { label: "reco A", feeder: { from: "query" }, packer: { size: 1 }, retries: 1 },
    recoB: { label: "reco B", feeder: { from: "query" }, packer: { size: 1 }, retries: 1 },
    compare: { feeder: { from: "recoA" }, packer: { size: 1, join: "recoB" }, output: "triangle", finalize: ["every partner joined"] },
  },
  outputs: { comparisons: { from: "compare", label: "comparisons" } },
}
```

The comparison step has two inputs, one from each reconstruction, but only one edge feeds its pool: the driving edge from reconstruction A. The other edge is joined by the packer, which finds for each input the partner file that reconstruction B produced from the same source file. The pool and the counters therefore count reconstruction A's outputs, and a file whose partner is missing waits, delayed, rather than sitting unpaired in the pool.

## The document

!!! info "CWL document (placeholder)"

    The document form is pending the `dirac:` hint schema of DX-ADR-007. This pattern needs:

    - a comparison step with two inputs, one sourcing each reconstruction step's output;
    - its `dirac:Transformation` hint naming the reconstruction A input as the one that drives the pool, and the joining packer with the partner transformation and the step input it fills.

## What happens

1. Both reconstructions are fed by the query and run independently.
2. When a parcel of either reaches `Done`, its output is recorded under that reconstruction's declared output. The comparison's edge feeder reads only reconstruction A's; reconstruction B's stay recorded and are never fed to the comparison.
3. For each input the packer walks the schema: the parcel that produced the file, that parcel's inputs and their source file, reconstruction B's input for the same source file, and the files its parcel produced. That partner goes into the parcel's parameters.
4. If reconstruction B has not yet processed the source file, the packer delays the input. If reconstruction B gave up on it, so that the input ended `NotProcessed` or `Problematic`, the packer marks the comparison's input `Problematic`, since no partner will come.
5. The comparison drains once reconstruction A is finished and every input has been packed or written off, and its finalizing check confirms that every file reconstruction B produced was joined by exactly one parcel.

## Finalizing checks

**comparison**
:   Every file reconstruction B produced was joined by exactly one parcel.

## See also

- [Adding ancestor files](ancestry.md) is the same packer shape with the partner found in the bookkeeping instead of the workgraph.
- [Splitting an output](splitting.md) is the opposite shape.
- The lookup itself is worked through in [DX-ADR-004](../../../adr/DX-ADR-004_schema.md#transformationoutputs-and-parceloutputs) and the packer in [DX-ADR-006](../../../adr/DX-ADR-006_extensions.md#packer).
