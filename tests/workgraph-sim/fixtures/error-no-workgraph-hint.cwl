# A CWL Workflow with no dirac:Workgraph hint: a plain workflow rather than a workgraph.
cwlVersion: v1.2
class: Workflow
id: no-workgraph-hint

$namespaces:
  dirac: https://diracgrid.org/cwl#

inputs:
  input-data:
    dirac:Feeder: {name: Query}
    type: {type: array, items: [File, string]}

steps:
  reco:
    hints:
      dirac:Transformation:
        packer: {name: BySize, args: {group_size: 2}}
    run: tools/reco.cwl
    in: {input-data: input-data}
    out: [processed]

outputs:
  datasets:
    outputSource: reco/processed
    type: File[]
