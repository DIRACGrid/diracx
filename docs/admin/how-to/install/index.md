# Introduction

These pages provide detailed instructions and best practices to have a running `DiracX` instance. If you start from nothing, they should be followed in order.

- [Install Kubernetes](install-kubernetes.md): how to install a minimal cluster.
- [Convert CS](convert-cs.md): how to generate the `DiracX` configuration from your `DIRAC` CS.
- [Installing](installing.md): how to install `DiracX`, that will do nothing to start with.
- [Register the Admin VO](register-the-admin-vo.md): Add the administrator VO, mandatory in `DiracX`
- [Register a VO](register-a-vo.md): integrate a VO that you already have in `DIRAC`
- [Embracing `DiracX`](embracing.md): last steps to redirect the traffic from `DIRAC` to `DiracX`

!!! important "We assume you already have a DIRAC v9 installation available"

    `DiracX` can only work alongside a `DIRAC` installation. We also assume that
    you have access to a `DIRAC` client, which comes with the `dirac` cli.
