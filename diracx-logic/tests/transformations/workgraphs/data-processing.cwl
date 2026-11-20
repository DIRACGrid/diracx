# Hints and fields written in list form, as cwltool --pack emits them
cwlVersion: v1.2
class: Workflow
label: Sprucing
$namespaces:
  dirac: https://diracgrid.org/cwl#

hints:
  - class: dirac:Workgraph
    schema_version: '1.0'
    type: Sprucing

inputs:
  - id: raw-data
    type: {type: array, items: [File, string]}
    dirac:Feeder:
      name: LHCbBookkeeping
      args: {configName: LHCb, configVersion: Collision26, fileType: RAW}
  - id: options
    type: string
    default: sprucing-options.py

steps:
  - id: sprucing
    hints:
      - class: dirac:Transformation
        packer: {name: FixedSize, args: {group_size: 2}}
        requirements_template: {priority: 2, output_se: CERN-BUFFER}
    run:
      class: CommandLineTool
      baseCommand: [lb-run, Moore]
      inputs:
        - id: input-data
          type: {type: array, items: [File, string]}
        - id: options
          type: string
      outputs:
        - id: spruced
          type: File[]
          outputBinding: {glob: '*.dst'}
    in:
      - id: input-data
        source: raw-data
      - id: options
        source: options
    out: [spruced]

outputs:
  - id: spruced
    type: File[]
    outputSource: sprucing/spruced
