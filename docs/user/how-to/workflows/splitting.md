# Splitting an output

Feed one step's output to two steps, and remove the files only once both have processed them.

## The workgraph

```workgraph
{
  name: "splitting",
  sources: { query: { files: 200 } },
  transformations: {
    reco: { feeder: { from: "query" }, packer: { size: 2 }, emit: "input" },
    selA: { label: "selection A", feeder: { from: "reco" }, packer: { size: 3 }, output: "triangle" },
    selB: { label: "selection B", feeder: { from: "reco" }, packer: { size: 3 }, output: "diamond" },
    removal: { kind: "removal", feeder: { from: "reco", after: ["selA", "selB"] }, packer: { size: 4 }, finalize: ["only NotProcessed files remain"] },
  },
  outputs: { a: { from: "selA", label: "selection A" }, b: { from: "selB", label: "selection B" } },
}
```

The reconstruction's output is consumed by two selections, each delivering a dataset, and by a removal. A file reaches all three pools when its parcel finishes; the removal's packer waits until both selections have processed it.

## The document

!!! info "CWL document (placeholder)"

    The document form is pending the `dirac:` hint schema of DX-ADR-007. This pattern needs:

    - two steps whose inputs both source the reconstruction step's output, which is ordinary CWL dataflow;
    - both selection outputs declared as workflow outputs, since an output that is neither consumed nor declared is a submission error;
    - a data-management declaration in the `dirac:Workgraph` hint against the reconstruction output for the removal, with its packer and initial state.

## What happens

1. When a reconstruction parcel reaches `Done`, its files are recorded once, under the reconstruction's declared output, and each of the three consumers reads them with its own bookmark.
2. Each selection's edge feeder picks up its rows and packs them independently. The two selections need not keep pace with each other.
3. The removal's edge feeder picks up its rows too, and its packer delays each file until the inputs for that file in both selections are `Processed`. A file that either selection wrote off as `NotProcessed` stays.
4. All three consumers drain once the reconstruction is finished, and the removal's finalizing check runs after the selections'.

## Finalizing checks

**removal**
:   The reconstruction output directory contains only files a selection left `NotProcessed`.

## Variations

- A consumer can take only one type of file, so an output can be split by type rather than duplicated, as in the fan-out on the [explanation page](../../explanations/transformation-system.md#edges).
- Without the removal, the reconstruction output is either a declared workflow output or is removed in-job once both consumers have registered their outputs.

## See also

- [Removing intermediates](removal.md) is the single-consumer case.
- [Joining two outputs](joining.md) is the opposite shape: two producers into one consumer.
