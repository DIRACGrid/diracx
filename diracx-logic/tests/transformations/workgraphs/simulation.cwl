# Packed as cwltool --pack writes it: the step runs another $graph entry
cwlVersion: v1.2
$namespaces:
  dirac: https://diracgrid.org/cwl#
$graph:
  - class: Workflow
    id: '#main'
    label: MC simulation
    doc: Gauss, one seed per job.
    hints:
      - class: https://diracgrid.org/cwl#Workgraph
        schema_version: '1.0'
        type: MCSimulation
    inputs:
      - id: '#main/events'
        doc: Seeds, issued until the requested number of events has been produced
        type: {type: array, items: [File, string]}
        https://diracgrid.org/cwl#Feeder:
          name: Seeds
          args: {target_events: 600000, events_per_seed: 1000}
    steps:
      - id: '#main/MCSimulation'
        label: MCSimulation
        doc: Gauss, one seed per job
        hints:
          - class: https://diracgrid.org/cwl#Transformation
            packer: {name: FixedSize, args: {group_size: 1}}
        run: '#gauss.cwl'
        in:
          - id: '#main/MCSimulation/seed'
            source: '#main/events'
        out: ['#main/MCSimulation/sim-files']
    outputs:
      - id: '#main/datasets'
        type: File[]
        outputSource: '#main/MCSimulation/sim-files'
  - class: CommandLineTool
    id: '#gauss.cwl'
    baseCommand: [lb-run, Gauss]
    inputs:
      - id: '#gauss.cwl/seed'
        type: [File, string]
        inputBinding: {position: 1}
    outputs:
      - id: '#gauss.cwl/sim-files'
        type: File[]
        outputBinding: {glob: '*.sim'}
