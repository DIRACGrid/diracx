cwlVersion: v1.2
class: Workflow
id: MC_2026_Bs2JpsiPhi_Sim12_scouted
label: MC simulation with a scout
doc: A sample is run and the workgraph approved before the rest of it is requested.

$namespaces:
  dirac: https://diracgrid.org/cwl#

hints:
  dirac:Workgraph:
    schema_version: '1.0'
    type: MCSimulation
    target: {output: datasets, files: 20}
    output_sandbox: ['prodConf_*.json', 'summary*.xml', 'prmon*', '*.log']
    approving:
    - {action: CheckSuccessRate, args: {min_success_rate: 0.9}}
    - {action: EstimateResourceUsage, args: {max_cpu_hours: 1000, max_memory_gb: 500}}
    - {action: ManualApproval, args: {role: mc_production_manager}}
    # The ladder belongs to the hook that climbs it, in the feeder's own unit
    hooks:
      ScoutingToApproving: {hook: ScoutInStages, args: {stages: [8000, 16000]}}

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
