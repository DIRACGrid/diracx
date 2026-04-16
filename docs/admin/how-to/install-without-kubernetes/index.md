# Introduction

These pages provide instructions for how to have a running `DiracX` instance without installing Kubernetes.
This is an alternative to the main installation guide, which assumes you have a Kubernetes cluster available.

!!! warning "This installation method is best-effort and not recommended"

    This installation method is intended to assist with migrating existing `DIRAC` instances to `DiracX` without the need to set up a Kubernetes cluster.

- [Minimal Requirements](minimal-requirements.md): minimal requirements for running `DiracX`
- [Preparing a container node](prepare-container-node.md): how to prepare a node for a container-based deployment.
- [Convert CS](../install/convert-cs.md): how to generate the `DiracX` configuration from your `DIRAC` CS.
- [Installing DiracX in a Container](installing-in-a-container.md): how to install `DiracX` in a container.
- [Register the Admin VO](../install/register-the-admin-vo.md): Add the administrator VO, mandatory in `DiracX`
- [Register a VO](../install/register-a-vo.md): integrate a VO that you already have in `DIRAC`
- [Embracing `DiracX`](../install/embracing.md): last steps to redirect the traffic from `DIRAC` to `DiracX`

!!! important "We assume you already have a DIRAC v9 installation available"

    `DiracX` can only work alongside a `DIRAC` installation. We also assume that
    you have access to a `DIRAC` client, which comes with the `dirac` cli.
