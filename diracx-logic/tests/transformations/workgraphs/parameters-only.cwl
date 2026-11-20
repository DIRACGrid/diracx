# A transformation without a feeder: its inputs arrive by other means
cwlVersion: v1.2
class: Workflow
$namespaces:
  dirac: https://diracgrid.org/cwl#

hints:
  dirac:Workgraph:
    schema_version: '1.0'
    type: Test

inputs:
  message: string

steps:
  echo:
    hints:
      dirac:Transformation:
        packer: {name: FixedSize, args: {group_size: 1}}
    run:
      class: CommandLineTool
      baseCommand: echo
      inputs:
        message: {type: string, inputBinding: {position: 1}}
      outputs: []
    in: {message: message}
    out: []

outputs: []
