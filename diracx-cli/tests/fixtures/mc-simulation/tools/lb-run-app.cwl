cwlVersion: v1.2
class: CommandLineTool
doc: Run one LHCb application step described by a prodConf.
requirements:
  InlineJavascriptRequirement: {}
  InitialWorkDirRequirement:
    listing:
      - entryname: prodConf.json
        entry: $(inputs["prod-conf"])
baseCommand: [lb-prod-run]
arguments: [prodConf.json]
inputs:
  prod-conf:
    type: string
    doc: The application, its version and options, as JSON
  input-data:
    type:
      - "null"
      - type: array
        items: [File, string]
    inputBinding:
      prefix: --input
  output-glob:
    type: string
outputs:
  output-data:
    type: File[]
    outputBinding:
      glob: $(inputs["output-glob"])
