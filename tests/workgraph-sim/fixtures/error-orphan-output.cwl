# A step output that is neither consumed by another step nor declared as a workgraph output.
cwlVersion: v1.2
class: Workflow
id: orphan-output

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
    out: [processed, leftovers]

outputs:
  datasets:
    outputSource: reco/processed
    type: File[]
