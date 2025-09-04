# Minimal requirements

In order to run DiracX, a few requirements must be met:

- **DIRAC v9**: DiracX can not yet work standalone, but alongside an existing DIRAC v9 installation. 
- **MySQL** (with MariaDB as a common alternative). Minimum MySQL version 8. Most of 
- **OpenSearch**
- **S3-compatible storage** for storing jobs' sandboxes
- **[Kubernetes](https://kubernetes.io/docs/tutorials/kubernetes-basics/)** for DiracX installation - this is the only supported way of running DiracX.
- An **IdP** supporting OAuth/OIDC (e.g. [Indigo IAM](https://indigo-iam.github.io/))

The following chapters will *NOT* cover: 
- DIRAC v9 installation. If you are new to DIRAC, defer to the [existing DIRAC documentation](https://dirac.diracgrid.org).
- [MySQL](https://dev.mysql.com/doc/refman/8.4/en/installing.html), [OpenSearch](https://docs.opensearch.org/latest/install-and-configure/install-opensearch/index/) and [S3](https://docs.min.io/enterprise/aistor-object-store/) installations.

Few suggestion will be given for Kubernetes installation. 
