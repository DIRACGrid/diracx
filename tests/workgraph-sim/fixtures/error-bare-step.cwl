# A step with no dirac:Transformation hint: a bare step is a submission-time error (DX-ADR-007).
cwlVersion: v1.2
class: Workflow
id: bare-step

$namespaces:
  dirac: https://diracgrid.org/cwl#

hints:
  dirac:Workgraph: {schema_version: '1.0'}

inputs:
  input-data:
    dirac:Feeder: {name: Query}
    type: {type: array, items: [File, string]}

steps:
  reco:
    hints:
      dirac:Transformation:
        packer: {name: BySize, args: {group_size: 2}}
    run: tools/reco.cwl
    in: {input-data: input-data}
    out: [processed]
  merge:
    run: tools/merge.cwl
    in: {input-data: reco/processed}
    out: [merged]

outputs:
  datasets:
    outputSource: merge/merged
    type: File[]
