# Packed, with the step's process inline in run and a job parameter input
cwlVersion: v1.2
$namespaces:
  dirac: https://diracgrid.org/cwl#
$graph:
  - class: Workflow
    id: '#main'
    label: Sprucing
    hints:
      - class: https://diracgrid.org/cwl#Workgraph
        schema_version: '1.0'
        type: Sprucing
    inputs:
      - id: '#main/raw-data'
        type: {type: array, items: [File, string]}
        https://diracgrid.org/cwl#Feeder:
          name: LHCbBookkeeping
          args: {configName: LHCb, configVersion: Collision26, fileType: RAW}
      - id: '#main/options'
        type: string
        default: sprucing-options.py
    steps:
      - id: '#main/sprucing'
        hints:
          - class: https://diracgrid.org/cwl#Transformation
            packer: {name: FixedSize, args: {group_size: 2}}
            requirements_template: {priority: 2, output_se: CERN-BUFFER}
        run:
          class: CommandLineTool
          baseCommand: [lb-run, Moore]
          inputs:
            - id: '#main/sprucing/run/input-data'
              type: {type: array, items: [File, string]}
            - id: '#main/sprucing/run/options'
              type: string
          outputs:
            - id: '#main/sprucing/run/spruced'
              type: File[]
              outputBinding: {glob: '*.dst'}
        in:
          - id: '#main/sprucing/input-data'
            source: '#main/raw-data'
          - id: '#main/sprucing/options'
            source: '#main/options'
        out: ['#main/sprucing/spruced']
    outputs:
      - id: '#main/spruced'
        type: File[]
        outputSource: '#main/sprucing/spruced'
