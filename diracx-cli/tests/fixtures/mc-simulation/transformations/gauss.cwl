cwlVersion: v1.2
class: Workflow
doc: Gauss, one seed per job.
inputs:
  seed:
    type:
      type: array
      items: [File, string]
outputs:
  sim-files:
    type: File[]
    outputSource: gauss/output-data
steps:
  gauss:
    run: ../tools/lb-run-app.cwl
    in:
      input-data: seed
      prod-conf:
        default: '{"application": {"name": "Gauss", "version": "v57r1"}, "event_type": "13144011"}'
      output-glob:
        default: "*.sim"
    out: [output-data]
