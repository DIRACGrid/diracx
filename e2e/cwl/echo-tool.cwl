cwlVersion: v1.2
class: CommandLineTool
label: echo-tool

hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User

baseCommand: [echo]

inputs:
  - id: message
    type: string
    inputBinding:
      position: 1

stdout: stdout.log
stderr: stderr.log

outputs:
  - id: stdout_log
    type: stdout
  - id: stderr_log
    type: stderr

$namespaces:
  dirac: https://diracgrid.org/cwl#
