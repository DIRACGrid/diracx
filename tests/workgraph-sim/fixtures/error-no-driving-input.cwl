# A step whose only input is a job parameter, so nothing drives its pool.
cwlVersion: v1.2
class: Workflow
id: no-driving-input

$namespaces:
  dirac: https://diracgrid.org/cwl#

hints:
  dirac:Workgraph: {schema_version: '1.0'}

inputs:
  output-prefix:
    default: '00012345_00006789'
    type: string

steps:
  reco:
    hints:
      dirac:Transformation:
        packer: {name: BySize, args: {group_size: 2}}
    run: tools/reco.cwl
    in: {output-prefix: output-prefix}
    out: [processed]

outputs:
  datasets:
    outputSource: reco/processed
    type: File[]
