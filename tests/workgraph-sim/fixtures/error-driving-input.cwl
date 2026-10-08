# driving_input naming an input that does not drive the pool at all.
cwlVersion: v1.2
class: Workflow
id: driving-input

$namespaces:
  dirac: https://diracgrid.org/cwl#

hints:
  dirac:Workgraph: {schema_version: '1.0'}

inputs:
  output-prefix:
    default: '00012345_00006789'
    type: string
  input-data:
    dirac:Feeder: {name: Query}
    type: {type: array, items: [File, string]}

steps:
  reco:
    hints:
      dirac:Transformation:
        driving_input: output-prefix
        packer: {name: BySize, args: {group_size: 2}}
    run: tools/reco.cwl
    in:
      input-data: input-data
      output-prefix: output-prefix
    out: [processed]

outputs:
  datasets:
    outputSource: reco/processed
    type: File[]
