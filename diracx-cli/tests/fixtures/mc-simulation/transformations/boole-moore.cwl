cwlVersion: v1.2
class: Workflow
doc: Boole and Moore over about 3 GB of simulated data per job.
requirements:
  SubworkflowFeatureRequirement: {}
inputs:
  input-data: File[]
outputs:
  reco-files:
    type: File[]
    outputSource: digi-reco/reco-files
steps:
  digi-reco:
    run: digi-reco.cwl
    in:
      input-data: input-data
    out: [reco-files]
