cwlVersion: v1.2
class: Workflow
label: two-step-workflow

hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User

inputs:
  - id: message
    type: string

steps:
  - id: greet
    run:
      class: CommandLineTool
      baseCommand: [echo]
      inputs:
        - id: msg
          type: string
          inputBinding:
            position: 1
      stdout: greet-stdout.log
      outputs:
        - id: log
          type: stdout
    in:
      - id: msg
        source: message
    out: [log]

  - id: shout
    run:
      class: CommandLineTool
      baseCommand: [bash, -c]
      arguments:
        - valueFrom: "cat $(inputs.infile.path) | tr '[:lower:]' '[:upper:]'"
          position: 0
      inputs:
        - id: infile
          type: File
      stdout: shout-stdout.log
      outputs:
        - id: log
          type: stdout
    in:
      - id: infile
        source: greet/log
    out: [log]

outputs:
  - id: final_output
    type: File
    outputSource: shout/log

$namespaces:
  dirac: https://diracgrid.org/cwl#
