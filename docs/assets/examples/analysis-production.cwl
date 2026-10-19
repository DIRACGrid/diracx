#!/usr/bin/env cwl-runner
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
  InlineJavascriptRequirement: {}
  SubworkflowFeatureRequirement: {}
  StepInputExpressionRequirement: {}
  MultipleInputFeatureRequirement: {}
  ResourceRequirement:
    coresMin: 1
    ramMin: 2048

hints:
  dirac:Workgraph:
    schema_version: '1.0'
    type: AnalysisProduction
    output_sandbox:
    - prodConf_*.json
    - prodConf_*.py
    - summary*.xml
    - prmon*
    - '*.log'

inputs:
  output-prefix:
    doc: Output file prefix (PPPPPPPP_JJJJJJJJ), injected per parcel at dispatch
    default: '00012345_00006789'
    type: string
  input-data:
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
  transformation_1:
    label: WGProduction
    doc: DaVinci tupling over the bookkeeping dataset
    hints:
      dirac:Transformation:
        packer: {name: BySize, args: {group_size: 2}}
    run: transformations/transformation-1.cwl
    in:
      input-data: input-data
      output-prefix: output-prefix
    out: [LB2DDSTP_ROOT]

  transformation_2:
    label: APMerge
    doc: Skim and merge the ntuples produced by WGProduction
    hints:
      dirac:Transformation:
        packer: {name: BySize, args: {group_size: 5}}
        actions:
          Finalizing: [{action: NoInputProcessedTwice}]
    run: transformations/transformation-2.cwl
    in:
      input-data: transformation_1/LB2DDSTP_ROOT
      output-prefix: output-prefix
    out: [LB2DDSTP_ROOT]

outputs:
  LB2DDSTP_ROOT:
    label: 'Output data: LB2DDSTP.ROOT'
    outputSource: transformation_2/LB2DDSTP_ROOT
    type: File[]
