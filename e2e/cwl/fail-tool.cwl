cwlVersion: v1.2
class: CommandLineTool
label: fail-tool

hints:
  - class: dirac:Job
    schema_version: "1.0"
    type: User

baseCommand: [bash, -c]
arguments:
  - valueFrom: "echo 'about to fail' && exit 42"
    position: 0

stdout: stdout.log
stderr: stderr.log

inputs: []
outputs:
  - id: stdout_log
    type: stdout
  - id: stderr_log
    type: stderr

$namespaces:
  dirac: https://diracgrid.org/cwl#
