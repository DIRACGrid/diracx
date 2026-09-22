cwlVersion: v1.2
class: Workflow
id: Sprucing_Collision26_26c2_histograms
label: Collecting histograms
doc: |-
  A job produces monitoring histograms beside its data. They are a second declared
  output rather than part of the deliverable, and a transformation of their own
  merges them.

$namespaces:
  dirac: https://diracgrid.org/cwl#

hints:
  dirac:Workgraph:
    schema_version: '1.0'
    type: Sprucing
    output_sandbox: ['prodConf_*.json', 'summary*.xml', '*.log']

inputs:
  raw-data:
    dirac:Feeder:
      name: LHCbBookkeeping
      args:
        conditions_dict: {configName: LHCb, configVersion: Collision26, inFileType: RAW}
        start_run: 285000
        end_run: 286500
    type: {type: array, items: [File, string]}

steps:
  sprucing:
    label: Sprucing
    doc: Moore, writing a MOOREHIST histogram beside each output
    hints:
      dirac:Transformation:
        packer: {name: ByRun, args: {group_size: 2}}
        actions:
          Finalizing: [{action: NoInputProcessedTwice}]
    run: transformations/moore-spruce.cwl
    in: {input-data: raw-data}
    out: [spruced, histograms]

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

  histogram-merge:
    label: histogram merge
    doc: hadd over the histograms of a run
    hints:
      dirac:Transformation:
        packer: {name: ByRun, args: {group_size: 8}}
    run: transformations/hadd.cwl
    in: {input-data: sprucing/histograms}
    out: [merged-histograms]

outputs:
  datasets:
    label: Spruced datasets
    outputSource: merge/merged
    type: File[]
  histograms:
    label: Merged histograms
    outputSource: histogram-merge/merged-histograms
    type: File[]
