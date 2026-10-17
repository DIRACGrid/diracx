cwlVersion: v1.2
class: Workflow
id: Stripping_Collision26_S40
label: RDST stripping · Collision26 · S40

$namespaces:
  dirac: https://diracgrid.org/cwl#

hints:
  dirac:Workgraph:
    schema_version: '1.0'
    type: Stripping
    output_sandbox: ['prodConf_*.json', 'summary*.xml', '*.log']
    data_management:
    - id: RDSTStaging
      operation: replicate
      files: rdst-data
      destination: BUFFER
      packer: {name: ByRun, args: {group_size: 10}}
    - id: RAWStaging
      operation: replicate
      files: raw-ancestors
      destination: BUFFER
      packer: {name: ByRun, args: {group_size: 10}}
    - id: BufferRemoval
      operation: remove
      files: rdst-data
      storage: BUFFER
      after: [stripping]
      packer: {name: BySize, args: {group_size: 20}}

inputs:
  rdst-data:
    dirac:Feeder:
      name: LHCbBookkeeping
      args:
        conditions_dict: {configName: LHCb, configVersion: Collision26, inFileType: RDST}
        start_run: 285000
        end_run: 286500
    type: {type: array, items: [File, string]}
  raw-ancestors:
    doc: The RAW ancestors of the RDST files, so that they can be staged too
    dirac:Feeder:
      name: LHCbAncestors
      args: {of: rdst-data, file_type: RAW}
    type: {type: array, items: [File, string]}

steps:
  stripping:
    label: Stripping
    doc: DaVinci over each RDST together with its RAW ancestor
    hints:
      dirac:Transformation:
        driving_input: input-data
        after: [RDSTStaging, RAWStaging]
        packer:
          name: AncestorLookup
          args: {group_size: 2, partner_input: ancestor-data, file_type: RAW}
        actions:
          Finalizing: [{action: NoInputProcessedTwice}]
    run: transformations/davinci-strip.cwl
    in:
      input-data: rdst-data
      ancestor-data: raw-ancestors
    out: [stripped]

  merge:
    label: Merge
    hints:
      dirac:Transformation:
        packer: {name: ByGroupSizeRun, args: {group_size: 5}}
        actions:
          Finalizing: [{action: NoInputProcessedTwice}]
    run: transformations/merge.cwl
    in: {input-data: stripping/stripped}
    out: [merged]

outputs:
  datasets:
    label: Stripped datasets
    outputSource: merge/merged
    type: File[]
