# Collecting histograms

Keep the small files every job produces alongside its data, such as monitoring histograms, and merge them into one per transformation. There are two ways, and they combine.

In every case the reconstruction job declares the histogram as a step output, so that it is registered and can travel an edge. Files kept through the output sandbox patterns are not registered and cannot be consumed by anything.

## The workgraph

=== "Merge step"

    A compute transformation consumes the histogram output along an edge and merges many into one, like any merge. It scales to any number of jobs and runs while the workgraph runs.

    ```workgraph
    {
      name: "merge step",
      sources: { query: { files: 160 } },
      transformations: {
        reco: { feeder: { from: "query" }, packer: { size: 2 }, artifact: true },
        merge: { feeder: { from: "reco" }, packer: { size: 3 } },
        histMerge: { label: "histogram merge", feeder: { from: "reco", port: "artifact" }, packer: { size: 5 }, run: [0.6, 1.2], output: "square" },
      },
      outputs: { datasets: { from: "merge", label: "datasets" }, hists: { from: "histMerge", label: "histograms" } },
    }
    ```

=== "Finalizing action"

    The histograms are collected as a workgraph output, and a finalizing action bound to the reconstruction merges them once it has drained. It needs no extra transformation but runs once, at the end, over everything.

    ```workgraph
    {
      name: "finalizing action",
      sources: { query: { files: 160 } },
      transformations: {
        reco: { feeder: { from: "query" }, packer: { size: 2 }, artifact: true, finalize: [{ name: "merge histograms", merge: "hists" }] },
        merge: { feeder: { from: "reco" }, packer: { size: 3 } },
      },
      outputs: { datasets: { from: "merge", label: "datasets" }, hists: { from: "reco", port: "artifact", label: "histograms" } },
    }
    ```

=== "Both"

    The merge step reduces the count while the workgraph runs, and the finalizing action merges the merge step's outputs into one.

    ```workgraph
    {
      name: "both",
      sources: { query: { files: 160 } },
      transformations: {
        reco: { feeder: { from: "query" }, packer: { size: 2 }, artifact: true },
        merge: { feeder: { from: "reco" }, packer: { size: 3 } },
        histMerge: { label: "histogram merge", feeder: { from: "reco", port: "artifact" }, packer: { size: 5 }, run: [0.6, 1.2], output: "square", finalize: [{ name: "merge histograms", merge: "hists" }] },
      },
      outputs: { datasets: { from: "merge", label: "datasets" }, hists: { from: "histMerge", label: "histograms" } },
    }
    ```

## The document

!!! info "CWL document (placeholder)"

    The document form is pending the `dirac:` hint schema of DX-ADR-007. This pattern needs:

    - the histogram declared as an output of the reconstruction step's tool, alongside the data output, and not only as an output-sandbox pattern;
    - for the merge step, a step whose input sources the histogram output, with its own `dirac:Transformation` hint;
    - for the finalizing action, the histogram output declared as a workflow output and the merging action in the reconstruction's finalizing list;
    - for both, the merge step's output declared as a workflow output and the action bound to the merge step.

## What happens

1. Every reconstruction parcel that reaches `Done` records its data files under the data output and its histogram under the histogram output, from which the merge, the histogram merge or the workgraph output collects them.
2. With a merge step, the histogram merge's edge feeder picks the histograms up and its packer groups them, and it drains with the rest of the chain.
3. With a finalizing action, the histograms accumulate until the workgraph moves to `Finalizing`. The action runs in the reconstruction's list, in DAG order, merges the collected files, and records `Passed`. If it fails, the transformation moves to `FinalizingBlocked` for an operator.
4. With both, the action runs in the histogram merge's list, after its producer has been finalised.

## See also

- [Basic simulation](simulation.md): the LHCb simulation merges its GAUSSHIST, BOOLEHIST and MOOREHIST files by finalizing actions.
