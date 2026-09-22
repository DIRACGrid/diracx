# scatter, which the playground drops on the floor.
cwlVersion: v1.2
class: Workflow
id: scatter

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
    scatter: input-data
    in: {input-data: input-data}
    out: [processed]

outputs:
  datasets:
    outputSource: reco/processed
    type: File[]
