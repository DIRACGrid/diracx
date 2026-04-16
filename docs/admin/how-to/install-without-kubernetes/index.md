# Introduction

These pages provide instructions for how to have a running `DiracX` instance without installing Kubernetes.
This is an alternative to the main installation guide, which assumes you have a Kubernetes cluster available.

!!! warning "This installation method is best-effort and not recommended"

    This installation method is intended to assist with migrating existing `DIRAC` instances to `DiracX` without the need to set up a Kubernetes cluster.

With respect to the original [minimal requirements](/docs/admin/how-to/install/minimal-requirements.md), the only addition is that you need a host on which to run the DiracX containers.

- [Preparing a container node](prepare-container-node.md): how to prepare a node for a container-based deployment.
- [Installing DiracX in a Container](installing-in-a-container.md): how to install `DiracX` in a container.

!!! important "We assume you already have a DIRAC v9 installation available"

    `DiracX` can only work alongside a `DIRAC` installation. We also assume that
    you have access to a `DIRAC` client, which comes with the `dirac` cli.
