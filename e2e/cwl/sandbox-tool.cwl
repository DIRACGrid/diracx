cwlVersion: v1.2
class: CommandLineTool
label: sandbox-tool

hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User
    input_sandbox:
      - source: script

baseCommand: [bash]

inputs:
  - id: script
    type: File
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
