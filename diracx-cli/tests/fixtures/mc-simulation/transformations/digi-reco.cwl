cwlVersion: v1.2
class: Workflow
doc: Boole then Moore, inside one job.
inputs:
  input-data: File[]
outputs:
  reco-files:
    type: File[]
    outputSource: moore/output-data
steps:
  boole:
    run: ../tools/lb-run-app.cwl
    in:
      input-data: input-data
      prod-conf:
        default: '{"application": {"name": "Boole", "version": "v46r1"}}'
      output-glob:
        default: "*.digi"
    out: [output-data]
  moore:
    run: ../tools/lb-run-app.cwl
    in:
      input-data: boole/output-data
      prod-conf:
        default: '{"application": {"name": "Moore", "version": "v57r2"}}'
      output-glob:
        default: "*.dst"
    out: [output-data]
