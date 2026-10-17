# The document every error fixture beside it is a small break of: it compiles with nothing to report at all.
cwlVersion: v1.2
class: Workflow
id: good
label: a workgraph with nothing to report

$namespaces:
  dirac: https://diracgrid.org/cwl#

hints:
  dirac:Workgraph:
    schema_version: '1.0'
    type: Test

inputs:
  input-data:
    dirac:Feeder:
      name: Query
      args: {file_type: DST}
    type: {type: array, items: [File, string]}

steps:
  reco:
    hints:
      dirac:Transformation:
        packer: {name: BySize, args: {group_size: 2}}
    run: tools/reco.cwl
    in: {input-data: input-data}
    out: [processed]

outputs:
  datasets:
    outputSource: reco/processed
    type: File[]
