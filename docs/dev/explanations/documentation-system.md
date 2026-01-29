# Documentation System

This page explains how the DiracX documentation system works, including its architecture, organization, and contribution guidelines.

## Overview

The DiracX documentation is built using [MkDocs](https://www.mkdocs.org/) with the [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/) theme. The documentation is hosted at [diracx.io](https://diracx.io/).

## The Divio Documentation System

DiracX documentation follows the [Divio documentation system](https://documentation.divio.com/), which organizes content into four distinct categories based on their purpose:

| Category          | Orientation            | Purpose                                                            |
| ----------------- | ---------------------- | ------------------------------------------------------------------ |
| **Tutorials**     | Learning-oriented      | Hands-on lessons that guide beginners through completing a project |
| **How-to guides** | Goal-oriented          | Step-by-step instructions for accomplishing specific tasks         |
| **Reference**     | Information-oriented   | Technical descriptions and specifications                          |
| **Explanation**   | Understanding-oriented | Background context and conceptual discussions                      |

Each category serves a different need:

- **Tutorials** teach newcomers by doing. They should be reliable, repeatable, and focused on learning rather than the end product.
- **How-to guides** solve specific problems. They assume the reader knows what they want to achieve and provide direct instructions.
- **Reference** material describes the machinery. It should be accurate, consistent, and structured for quick lookup.
- **Explanation** discusses topics in depth. It provides context, alternatives, and reasoning behind design decisions.

## Multi-Repository Architecture

DiracX documentation is aggregated from multiple repositories using the [`mkdocs-diracx-plugin`](https://github.com/DIRACGrid/mkdocs-diracx-plugin). This allows each repository to maintain its own documentation while presenting a unified site to users.

| Repository                                                    | Default Branch | Documentation Focus                                        |
| ------------------------------------------------------------- | -------------- | ---------------------------------------------------------- |
| [`diracx`](https://github.com/DIRACGrid/diracx)               | `main`         | Core documentation, API reference, developer guides        |
| [`diracx-charts`](https://github.com/DIRACGrid/diracx-charts) | `master`       | Helm charts, installation guides, deployment documentation |
| [`diracx-web`](https://github.com/DIRACGrid/diracx-web)       | `main`         | Web interface documentation, UI guides                     |

### How the Plugin Works

The `mkdocs-diracx-plugin` operates during the MkDocs build process:

1. Copies the primary documentation from the `diracx` repository
2. Clones or copies documentation from each configured external repository
3. Merges the documentation trees together
4. Tracks file origins to generate correct edit URLs

### File Precedence

When files exist in multiple repositories at the same path, later repositories in the configuration override earlier ones. The order in `mkdocs.yml` is:

1. `diracx` (base)
2. `diracx-charts` (overlays on top)
3. `diracx-web` (overlays on top)

This allows downstream repositories to extend or replace documentation pages as needed.

## Three Audiences

The documentation is organized around three primary audiences:

### Users (`/user/`)

People who interact with DiracX to submit jobs, manage data, or consume services. They need to understand:

- How to authenticate and access the system
- How to use the web interface
- How to use command-line tools and APIs
- Common workflows and best practices

### Administrators (`/admin/`)

People who install, configure, and maintain DiracX deployments. They need to understand:

- Installation requirements and procedures
- Configuration options
- Security and authentication setup
- Monitoring and troubleshooting

### Developers (`/dev/`)

People who contribute code to DiracX or build extensions. They need to understand:

- Development environment setup
- Architecture and design patterns
- Testing strategies
- Contribution guidelines

## Navigation Structure

The navigation structure is defined entirely in the primary `diracx` repository's `mkdocs.yml` file. External repositories contribute documentation files but do not modify the navigation.

When adding new pages:

1. Create the markdown file in the appropriate repository's `docs/` directory
2. Add the navigation entry to `diracx/mkdocs.yml`
3. Follow the Divio system to place the page in the correct category

## Edit URLs

Each documentation page includes an "edit" button that links directly to the source file on GitHub. The `mkdocs-diracx-plugin` tracks which repository each file originates from and generates the correct edit URL, pointing to the appropriate repository and branch.

## Building Documentation Locally

To build and preview the documentation locally:

```bash
pixi run mkdocs serve
```

This will:

1. Aggregate documentation from all configured repositories
2. Build the MkDocs site
3. Start a local server with live reload

Visit `http://localhost:8000` to preview the documentation.
