cwlVersion: v1.2
class: Workflow
id: MC_2026_Bs2JpsiPhi_Sim12
label: MC simulation
doc: Gauss, one seed per job.

$namespaces:
  dirac: https://diracgrid.org/cwl#

hints:
  dirac:Workgraph:
    schema_version: '1.0'
    type: MCSimulation

inputs:
  events:
    doc: Seeds, issued until the requested number of events has been produced
    dirac:Feeder:
      name: Seeds
      args:
        target_events: 600000
        events_per_seed: 1000
    type: {type: array, items: [File, string]}

steps:
  MCSimulation:
    label: MCSimulation
    doc: Gauss, one seed per job
    hints:
      dirac:Transformation:
        packer: {name: FixedSize, args: {group_size: 1}}
    run:
      class: CommandLineTool
      baseCommand: [lb-run, Gauss]
      inputs:
        seed: {type: [File, string], inputBinding: {position: 1}}
      outputs:
        sim-files: {type: 'File[]', outputBinding: {glob: '*.sim'}}
    in: {seed: events}
    out: [sim-files]

outputs:
  datasets:
    outputSource: MCSimulation/sim-files
    type: File[]
