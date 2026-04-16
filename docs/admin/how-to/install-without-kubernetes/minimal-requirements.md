# Minimal requirements

In order to run DiracX alongside an existing DIRAC v9 installation without a Kubernetes cluster, we recommend having:

- **DIRAC v9**: DiracX can not yet work standalone, but alongside an existing DIRAC v9 installation. Note that DIRAC v9 has as dependencies:
    - **MySQL** Minimum MySQL version 8 or MariaDB equivalent.
    - **OpenSearch** version 2 or 3.
- **S3-compatible storage** for storing jobs' sandboxes
- An **IdP** supporting OAuth/OIDC (e.g. [Indigo IAM](https://indigo-iam.github.io/))
- A host on which to run the DiracX containers. See [Prepare a node for container deployment](prepare-container-node.md) for how to set it up.

This container-based deployment has only been tested in conjunction with a DIRAC v9.0 deployment.

The following chapters will *NOT* cover:

- DIRAC v9 installation. If you are new to DIRAC, defer to the [existing DIRAC documentation](https://dirac.diracgrid.org).
- The installation of dependencies (MySQL, OpenSearch, object store). If you have hosted services available, we recommend using them.
