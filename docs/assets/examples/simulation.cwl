cwlVersion: v1.2
class: Workflow
id: MC_2026_Bs2JpsiPhi_Sim12_plain
label: MC simulation
doc: Produce a requested number of events, reconstruct them, and merge the reconstructed files into the deliverable.

$namespaces:
  dirac: https://diracgrid.org/cwl#

hints:
  dirac:Workgraph:
    schema_version: '1.0'
    type: MCSimulation
    target: {output: datasets, files: 20}
    output_sandbox: ['prodConf_*.json', 'summary*.xml', 'prmon*', '*.log']

inputs:
  events:
    doc: Seeds, issued until the requested number of events has been produced
    dirac:Feeder:
      name: Seeds
      args:
        target_events: 600000
        events_per_seed: 1000
        batch: 60000
        max_in_flight: 300000
    type: {type: array, items: [File, string]}

steps:
  MCSimulation:
    label: MCSimulation
    doc: Gauss, one seed per job
    hints:
      dirac:Transformation:
        packer: {name: PerInput}
        actions:
          Finalizing: [{action: NoInputProcessedTwice}]
    run: transformations/gauss.cwl
    in: {seed: events}
    out: [sim-files]

  MCReconstruction:
    label: MCReconstruction
    doc: Boole and Moore over about 3 GB of simulated data per job
    hints:
      dirac:Transformation:
        packer: {name: ByGroupSizeRun, args: {group_size: 3, keep_storage_together: true}}
        actions:
          Finalizing: [{action: NoInputProcessedTwice}]
    run: transformations/boole-moore.cwl
    in: {input-data: MCSimulation/sim-files}
    out: [reco-files]

  MCMerge:
    label: MCMerge
    doc: Merge about 10 GB of reconstructed data per job
    hints:
      dirac:Transformation:
        packer: {name: ByGroupSizeRun, args: {group_size: 4, keep_storage_together: true}}
        actions:
          Finalizing: [{action: NoInputProcessedTwice}]
    run: transformations/merge.cwl
    in: {input-data: MCReconstruction/reco-files}
    out: [merged]

outputs:
  datasets:
    label: Merged MC datasets
    outputSource: MCMerge/merged
    type: File[]
