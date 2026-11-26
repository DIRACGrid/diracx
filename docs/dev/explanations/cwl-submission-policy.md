# What DiracX accepts in a CWL process

A submitted document that is valid, self-contained CWL
([CWL process identity](cwl-process-identity.md)) is then checked against
what DiracX can run on every one of its backends
(`diracx.core.cwl.check_policy`). Every violation is listed in one error,
each naming the `$graph` entry, or the step running an inline body, it is
in.

- **Requirements only from an allow-list:** `DockerRequirement`,
    `EnvVarRequirement`, `InitialWorkDirRequirement`,
    `InlineJavascriptRequirement`, `LoadListingRequirement`,
    `MultipleInputFeatureRequirement`, `NetworkAccess`, `ResourceRequirement`,
    `ScatterFeatureRequirement`, `SchemaDefRequirement`,
    `ShellCommandRequirement`, `StepInputExpressionRequirement`,
    `SubworkflowFeatureRequirement` and `ToolTimeLimit`. A runner must honour
    a requirement or fail, so only what every backend provides is accepted.
    The other CWL v1.2 classes, `InplaceUpdateRequirement`,
    `SoftwareRequirement` and `WorkReuse`, are not: a backend cannot be relied
    on to update inputs in place, install software on the worker node or
    reuse earlier results.
- **Hints only from CWL v1.2 and DiracX's own `dirac:` classes.** A runner
    may ignore a hint, but cwltool fetches the IRI of a class it does not know.
- **`dirac:` classes only as hints**, so that a runner which does not know
    them can still run the document (DX-ADR-007).
- **No `DockerRequirement` with `dockerFile`, `dockerImport` or
    `dockerLoad`**, in requirements or hints: they build an image or fetch
    one from a URL on the worker node, which Apptainer cannot do there.
    Publish the image and name it with `dockerPull` (a registry image, or an
    unpacked image on CVMFS) or `dockerImageId`.
- **No `Operation`:** it declares inputs and outputs but nothing to run.
- **At most 256 kB per `$graph` entry and 1 MiB for the document**, in
    canonical JSON. A year of LHCb submissions peaks below 20 kB; large data
    belongs in a sandbox or a `File` input. The document limit also bounds
    each stored process, an entry with everything it runs.
- **Requirements and hints given as a list or map of objects.** The policy
    runs before the schema is validated, so it refuses what it cannot read
    rather than fail on it.

The lists are kept narrow on purpose: accepting more later is safe, but
refusing what was accepted would break documents already stored.
`InlineJavascriptRequirement` needs a JavaScript engine on the worker node,
QuickJS or Node.js, which the runner must provide rather than fetch.
