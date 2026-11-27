# A single process document, as cwltool --pack writes a workflow that runs
# nothing outside itself; no input is fed, they arrive by other means
cwlVersion: v1.2
class: Workflow
$namespaces:
  dirac: https://diracgrid.org/cwl#
hints:
  https://diracgrid.org/cwl#Workgraph:
    schema_version: '1.0'
    type: Test
inputs:
  message: string
steps:
  echo:
    hints:
      https://diracgrid.org/cwl#Transformation:
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
