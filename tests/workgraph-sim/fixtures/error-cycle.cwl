# A cycle in the dataflow: each step is fed from the other.
cwlVersion: v1.2
class: Workflow
id: cycle

$namespaces:
  dirac: https://diracgrid.org/cwl#

hints:
  dirac:Workgraph: {schema_version: '1.0'}

steps:
  reco:
    hints:
      dirac:Transformation:
        packer: {name: BySize, args: {group_size: 2}}
    run: tools/reco.cwl
    in: {input-data: merge/merged}
    out: [processed]
  merge:
    hints:
      dirac:Transformation:
        packer: {name: BySize, args: {group_size: 3}}
    run: tools/merge.cwl
    in: {input-data: reco/processed}
    out: [merged]

outputs:
  datasets:
    outputSource: merge/merged
    type: File[]
