# Minimal requirements

In order to run DiracX in production we recommend having:

- **DIRAC v9**: DiracX can not yet work standalone, but alongside an existing DIRAC v9 installation. Note that DIRAC v9 has as dependencies:
    - **MySQL** Minimum MySQL version 8 or MariaDB equivalent.
    - **OpenSearch** version 2 or 3.
- **S3-compatible storage** for storing jobs' sandboxes
- An **IdP** supporting OAuth/OIDC (e.g. [Indigo IAM](https://indigo-iam.github.io/))

At the time of writing, the only fully supported way of running DiracX is through **[Kubernetes](https://kubernetes.io/docs/tutorials/kubernetes-basics/)**. If you need to decouple your DIRAC v9 upgrade from a Kubernetes deployment, see [Installation without Kubernetes](../install-without-kubernetes/index.md).

The following chapters will *NOT* cover:

- DIRAC v9 installation. If you are new to DIRAC, defer to the [existing DIRAC documentation](https://dirac.diracgrid.org).
- The installation of dependencies (MySQL, OpenSearch, object store). If you have hosted services available, we recommend using them.
