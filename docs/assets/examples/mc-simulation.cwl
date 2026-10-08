cwlVersion: v1.2
class: Workflow
id: MC_2026_Bs2JpsiPhi_Sim12
label: MC 2026 · Bs→J/ψφ · Sim12
doc: One billion events, simulated, reconstructed and merged.

$namespaces:
  dirac: https://diracgrid.org/cwl#

hints:
  dirac:Workgraph:
    schema_version: '1.0'
    type: MCSimulation
    target: {output: datasets, files: 200}
    output_sandbox: ['prodConf_*.json', 'summary*.xml', 'prmon*', '*.log']
    approving:
    - {action: CheckSuccessRate, args: {min_success_rate: 0.9, min_passed: 80}}
    - {action: EstimateResourceUsage, args: {max_cpu_hours: 1000, max_memory_gb: 500}}
    - {action: PPGApproval}
    - {action: ManualApproval, args: {role: mc_production_manager}}
    data_management:
    - id: MCSimulationRemoval
      operation: remove
      files: MCSimulation/sim-files
      after: [MCReconstruction]
      packer: {name: BySize, args: {group_size: 20}}
      actions:
        Finalizing:
        - {action: OnlyUnprocessedRemain, args: {consumer: MCReconstruction}}
        - {action: RemovedFilesHaveNoReplica, args: {catalogue: bookkeeping}}
    - id: MCReconstructionRemoval
      operation: remove
      files: MCReconstruction/reco-files
      after: [MCMerge]
      packer: {name: BySize, args: {group_size: 20}}
      actions:
        Finalizing:
        - {action: OnlyUnprocessedRemain, args: {consumer: MCMerge}}
        - {action: RemovedFilesHaveNoReplica, args: {catalogue: bookkeeping}}
    - id: OutputReplication
      operation: replicate
      files: datasets
      destination: [CERN-DST, RAL-DST]
      packer: {name: BySize, args: {group_size: 5}}
      actions:
        Finalizing: [{action: EveryInputSettled}]

inputs:
  events:
    doc: Seeds, issued until the requested number of events has been produced
    dirac:Feeder:
      name: Seeds
      args:
        target_events: 1000000000
        events_per_seed: 1000
        scouting_events: 100000
        batch: 10000
        max_in_flight: 10000000
    type: {type: array, items: [File, string]}

steps:
  MCSimulation:
    label: MCSimulation
    doc: Gauss, one seed per job
    hints:
      dirac:Transformation:
        packer: {name: PerInput}
        actions:
          Finalizing: [{action: NoInputProcessedTwice}, {action: MergeHistograms, args: {output: GAUSSHIST}}]
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
          Finalizing:
          - {action: NoInputProcessedTwice}
          - {action: MergeHistograms, args: {output: BOOLEHIST}}
          - {action: MergeHistograms, args: {output: MOOREHIST}}
    run: transformations/boole-moore.cwl
    in: {input-data: MCSimulation/sim-files}
    out: [reco-files]

  MCMerge:
    label: MCMerge
    doc: Merge about 10 GB of reconstructed data per job
    hints:
      dirac:Transformation:
        packer: {name: ByGroupSizeRun, args: {group_size: 10, keep_storage_together: true}}
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
