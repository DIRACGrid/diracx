# Two pool-driving inputs and no driving_input to say which of them is the pool.
cwlVersion: v1.2
class: Workflow
id: ambiguous-driving-input

$namespaces:
  dirac: https://diracgrid.org/cwl#

hints:
  dirac:Workgraph: {schema_version: '1.0'}

inputs:
  rdst-data:
    dirac:Feeder: {name: Query, args: {file_type: RDST}}
    type: {type: array, items: [File, string]}
  raw-ancestors:
    dirac:Feeder: {name: Ancestors, args: {of: rdst-data, file_type: RAW}}
    type: {type: array, items: [File, string]}

steps:
  stripping:
    hints:
      dirac:Transformation:
        packer: {name: AncestorLookup, args: {group_size: 2, partner_input: ancestor-data}}
    run: tools/strip.cwl
    in:
      input-data: rdst-data
      ancestor-data: raw-ancestors
    out: [stripped]

outputs:
  datasets:
    outputSource: stripping/stripped
    type: File[]
