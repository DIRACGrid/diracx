## Embracing DiracX

TODO:

enable the vo -> gives you token
enable the sandbox -> offload the sandboxes
enable a service once all the VOs have been enabled -> use the legacy clients

### Enable the sandbox

!!! warning Enable the DiracX SandboxStore in the DIRAC CS

    The `DIRAC` clients still interact with the DIRAC services to proxy the sandbox files to S3. New writes only go to S3, and read can be done from both. Eventually, given the periodic cleanup of sandboxes, all the files will be on S3 only.
