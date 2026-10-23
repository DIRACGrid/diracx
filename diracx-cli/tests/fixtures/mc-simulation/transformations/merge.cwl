cwlVersion: v1.2
class: Workflow
doc: Merge about 10 GB of reconstructed data per job.
inputs:
  input-data: File[]
outputs:
  merged:
    type: File[]
    outputSource: merge/output-data
steps:
  merge:
    run: ../tools/lb-run-app.cwl
    in:
      input-data: input-data
      prod-conf:
        default: '{"application": {"name": "LHCb", "version": "v57r2"}, "merge": true}'
      output-glob:
        default: "*.dst"
    out: [output-data]
