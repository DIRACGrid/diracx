cwlVersion: v1.2
class: Workflow
id: Reprocessing_Collision26_comparison
label: Comparing two reconstructions
doc: |-
  The same files are reconstructed twice, and a third transformation compares each
  pair. Its packer waits for the partner of every input, and sets aside the inputs
  whose partner never arrives.

$namespaces:
  dirac: https://diracgrid.org/cwl#

hints:
  dirac:Workgraph:
    schema_version: '1.0'
    type: Reprocessing
    output_sandbox: ['summary*.xml', '*.log']

inputs:
  input-data:
    dirac:Feeder:
      name: LHCbBookkeeping
      args:
        conditions_dict: {configName: LHCb, configVersion: Collision26, inFileType: RDST}
        start_run: 285000
        end_run: 286500
    type: {type: array, items: [File, string]}

steps:
  reconstruction-a:
    label: reconstruction A
    doc: Moore, the reconstruction in production
    hints:
      dirac:Transformation:
        packer: {name: BySize, args: {group_size: 1}}
    run: transformations/moore-a.cwl
    in: {input-data: input-data}
    out: [reconstructed]

  reconstruction-b:
    label: reconstruction B
    doc: Moore, the candidate reconstruction
    hints:
      dirac:Transformation:
        packer: {name: BySize, args: {group_size: 1}}
    run: transformations/moore-b.cwl
    in: {input-data: input-data}
    out: [reconstructed]

  comparison:
    label: comparison
    doc: One job per file, over both reconstructions of it
    hints:
      dirac:Transformation:
        driving_input: input-a
        packer: {name: ByPartner, args: {group_size: 1, partner_input: input-b}}
        actions:
          Finalizing: [{action: EveryPartnerJoined}]
    run: transformations/compare.cwl
    in:
      input-a: reconstruction-a/reconstructed
      input-b: reconstruction-b/reconstructed
    out: [comparisons]

outputs:
  comparisons:
    label: Comparisons
    outputSource: comparison/comparisons
    type: File[]
