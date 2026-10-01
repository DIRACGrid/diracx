cwlVersion: v1.2
class: Workflow
id: Sprucing_Collision26_26c2
label: Sprucing · Collision26 · Spruce26c2
doc: Selects and slims the raw data stream as data-taking continues.

$namespaces:
  dirac: https://diracgrid.org/cwl#

hints:
  dirac:Workgraph:
    schema_version: '1.0'
    type: Sprucing
    output_sandbox: ['prodConf_*.json', 'summary*.xml', '*.log']
    # The workgraph scouts because this list exists: there is something to judge,
    # and the steps below are something to scout.
    approving:
    - {action: CheckSuccessRate, args: {min_success_rate: 0.95}}
    - {action: EstimateResourceUsage}
    - {action: ManualApproval, args: {role: data_processing_manager}}
    data_management:
    - id: StagingReplication          # sprucing waits on it, so it runs in the scout
      operation: replicate
      files: raw-data
      destination: BUFFER
      packer: {name: ByRun, args: {group_size: 10}}
    - id: BufferRemoval               # downstream of sprucing, so it starts on approval
      operation: remove
      files: raw-data
      storage: BUFFER
      after: [sprucing]
      packer: {name: BySize, args: {group_size: 20}}
    - id: OutputReplication           # acts on a workgraph output, so it starts on approval
      operation: replicate
      files: datasets
      destination: [CERN-DST, GRIDKA-DST]
      packer: {name: BySize, args: {group_size: 5}}

inputs:
  raw-data:
    doc: The run range moves forward as data-taking continues
    dirac:Feeder:
      name: LHCbBookkeeping
      args:
        conditions_dict:
          configName: LHCb
          configVersion: Collision26
          inFileType: RAW
        start_run: 285000
        end_run: 286500
        scouting_runs: [285000, 285001, 285020, 286104]
    type: {type: array, items: [File, string]}

steps:
  sprucing:
    label: Sprucing
    doc: Moore, once the file has reached the buffer
    hints:
      dirac:Transformation:
        after: [StagingReplication]
        packer: {name: ByRun, args: {group_size: 2}}
        actions:
          Finalizing: [{action: NoInputProcessedTwice}]
    run: transformations/moore-spruce.cwl
    in: {input-data: raw-data}
    out: [spruced]

  merge:
    label: Merge
    hints:
      dirac:Transformation:
        packer: {name: ByGroupSizeRun, args: {group_size: 5}}
        actions:
          Finalizing: [{action: NoInputProcessedTwice}]
    run: transformations/merge.cwl
    in: {input-data: sprucing/spruced}
    out: [merged]

outputs:
  datasets:
    label: Spruced datasets
    outputSource: merge/merged
    type: File[]
