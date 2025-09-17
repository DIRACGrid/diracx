# Minimal requirements

In order to run DiracX in production we recommend having:

- **DIRAC v9**: DiracX can not yet work standalone, but alongside an existing DIRAC v9 installation.
- **MySQL** Minimum MySQL version 8 or MariaDB equivalent.
- **OpenSearch**
- **S3-compatible storage** for storing jobs' sandboxes
- **[Kubernetes](https://kubernetes.io/docs/tutorials/kubernetes-basics/)** for DiracX installation - this is the only supported way of running DiracX.
- An **IdP** supporting OAuth/OIDC (e.g. [Indigo IAM](https://indigo-iam.github.io/))

The following chapters will *NOT* cover:

- DIRAC v9 installation. If you are new to DIRAC, defer to the [existing DIRAC documentation](https://dirac.diracgrid.org).
- The installation of dependencies (MySQL, OpenSearch, object store). If you have hosted services available, we recommend using them.
