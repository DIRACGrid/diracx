#!/usr/bin/env cwl-runner
# docs/assets/examples/analysis-production.cwl written the way the ADRs write a workgraph: every idmap field as a list
# carrying `id`, and the hints as a list of `class:` objects. It must compile to the same spec as the mapping form.
cwlVersion: v1.2
class: Workflow
id: AnaProd_lb2ddstp_2026_data_lb2ddstp_26_MagUp_Sp26c2a
label: AnaProd#lb2ddstp_2026_data#lb2ddstp_26_MagUp_Sp26c2a
doc: |-
  LHCb Analysis Production.
    Event type 94000000, processing pass Real Data/Sprucing26c2a,
    file type B2OC.DST, config LHCb/Collision26.

$namespaces:
  dirac: https://diracgrid.org/cwl#

requirements:
- class: InlineJavascriptRequirement
- class: SubworkflowFeatureRequirement
- class: StepInputExpressionRequirement
- class: MultipleInputFeatureRequirement
- class: ResourceRequirement
  coresMin: 1
  ramMin: 2048

hints:
- class: dirac:Workgraph
  schema_version: '1.0'
  type: AnalysisProduction
  output_sandbox:
  - prodConf_*.json
  - prodConf_*.py
  - summary*.xml
  - prmon*
  - '*.log'

inputs:
- id: output-prefix
  doc: Output file prefix (PPPPPPPP_JJJJJJJJ), injected per parcel at dispatch
  default: '00012345_00006789'
  type: string
- id: input-data
  doc: Evaluated by the feeder; the files are never listed in the document
  dirac:Feeder:
    name: LHCbBookkeeping
    args:
      event_type: '94000000'
      conditions_dict:
        configName: LHCb
        configVersion: Collision26
        inFileType: B2OC.DST
        inProPass: Real Data/Sprucing26c2a
      conditions_description: Beam6800GeV-VeloClosed-MagUp
  type:
    type: array
    items: [File, string]

steps:
- id: transformation_1
  label: WGProduction
  doc: DaVinci tupling over the bookkeeping dataset
  hints:
  - class: dirac:Transformation
    packer: {name: BySize, args: {group_size: 2}}
  run: transformations/transformation-1.cwl
  in:
  - {id: input-data, source: input-data}
  - {id: output-prefix, source: output-prefix}
  out: [LB2DDSTP_ROOT]

- id: transformation_2
  label: APMerge
  doc: Skim and merge the ntuples produced by WGProduction
  hints:
  - class: dirac:Transformation
    packer: {name: BySize, args: {group_size: 5}}
    actions:
      Finalizing: [{action: NoInputProcessedTwice}]
  run: transformations/transformation-2.cwl
  in:
  - {id: input-data, source: transformation_1/LB2DDSTP_ROOT}
  - {id: output-prefix, source: output-prefix}
  out: [LB2DDSTP_ROOT]

outputs:
- id: LB2DDSTP_ROOT
  label: 'Output data: LB2DDSTP.ROOT'
  outputSource: transformation_2/LB2DDSTP_ROOT
  type: File[]
