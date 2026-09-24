# Adding ancestor files

Give each job a second input that is not in the pool but is found by lookup, such as the RAW file an RDST must be processed with. This extends [Processing data from a query](data-processing.md).

An RDST is the output of the reconstruction software. It is missing parts of the raw detector data, so to reconstruct the events it must be processed together with the corresponding RAW file.

## The workgraph

```workgraph
{
  name: "RDST stripping",
  workgraph: { scouting: { count: 8 } },
  sources: { rdst: { label: "RDST query", files: 160, ancestors: "raw" }, raw: { label: "RAW query", files: 160, types: ["square"], colours: 3 } },
  transformations: {
    stageRdst: { kind: "replication", label: "RDST staging", feeder: { from: "rdst" }, packer: { size: 2 } },
    stageRaw: { kind: "replication", label: "RAW staging", feeder: { from: "raw" }, packer: { size: 2 } },
    strip: { label: "stripping", feeder: { from: "rdst", after: ["stageRdst", "stageRaw"] }, packer: { size: 2, lookup: "raw" }, finalize: ["no input used twice"] },
    merge: { feeder: { from: "strip" }, packer: { size: 3 }, finalize: ["no input used twice"] },
    replication: { kind: "replication", feeder: { from: "merge" }, packer: { size: 2 }, hold: "approval" },
  },
  outputs: { datasets: { from: "merge", label: "datasets" } },
}
```

Two staging replications, one for the RDST files and one for the RAW files. The stripping transformation's pool holds only RDST files, fed by the same query as the RDST staging. Its packer looks up the RAW ancestor of each RDST and adds it to the parcel, drawn as an outlined shape in the parcel's corner, and delays the RDST until both files are in the buffer.

## The document

!!! info "CWL document (placeholder)"

    The document form is pending the `dirac:` hint schema of DX-ADR-007. This pattern adds to the staged document:

    - a second workflow input for the RAW files with its own `dirac:Feeder` hint, either a bookkeeping query of its own or one derived from the ancestors of the RDST query, as the VO's feeder extension decides;
    - a second data-management declaration against that input for the RAW staging;
    - two inputs on the stripping step, with the `dirac:Transformation` hint naming the RDST input as the one that drives the pool and the ancestry packer, with its bookkeeping lookup, filling the other.

## What happens

1. The RDST staging and the RAW staging each copy their files to the buffer, fed by their own queries.
2. The stripping feeder yields the RDST files, so the pool, and the counters, count RDSTs.
3. For each RDST the packer asks the bookkeeping for its ancestors. It delays the RDST until the RDST staging has processed it and the RAW staging has processed its ancestor, then packs it with the ancestor written into the parcel's parameters for the RAW step input.
4. The job runs with both files. The rest of the chain is as for any processing workgraph.

## Finalizing checks

**stripping**
:   No input was used twice.

**merge**
:   No input was used twice.

## See also

- [Joining two outputs](joining.md) is the same packer shape with the partner found inside the workgraph instead of in the bookkeeping.
