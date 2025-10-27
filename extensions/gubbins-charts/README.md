# gubbins-charts

This is an umbrella Helm chart for managing the Gubbins extension of DiracX independently of the base DiracX chart version.

Gubbins is an example extension meant as a tutorial and test for extending DiracX with community-specific functionality.

## Overview

This chart extends the base `diracx` chart by:

- Configuring gubbins-specific container images
- Adding the LollygagDB database
- Setting up gubbins-specific deployment settings

## Prerequisites

- Kubernetes cluster (1.19+)
- Helm 3.0+
- Access to the DiracX charts repository at https://charts.diracgrid.org/

## Installation

### Add the DiracX Helm repository

```bash
helm repo add diracx https://charts.diracgrid.org/
helm repo update
```

### Install dependencies

```bash
helm dependency build
```

### Install the chart

```bash
helm install gubbins . --values values.yaml
```

### Install with custom values

```bash
helm install gubbins . --values values.yaml --values custom-values.yaml
```

## Configuration

The main configuration is in `values.yaml`. Key settings include:

- `diracx.global.images.services`: Container image registry for gubbins services
- `diracx.global.images.tag`: Image tag to use (default: "dev")
- `diracx.diracx.hostname`: Hostname for the gubbins deployment
- `diracx.diracx.sqlDbs.dbs.LollygagDB`: Gubbins-specific database configuration
- `diracx.developer.enabled`: Enable developer mode for testing

## Upgrading

```bash
helm upgrade gubbins . --values values.yaml
```

## Uninstalling

```bash
helm uninstall gubbins
```

## Development

This chart is part of the DiracX extensions ecosystem and serves as an example for communities wanting to extend DiracX functionality.

For more information about creating DiracX extensions, see the [DiracX documentation](https://github.com/diracgrid/diracx).
