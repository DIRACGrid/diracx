# Dependency management

DiracX uses [Renovate](https://docs.renovatebot.com/) to keep dependencies up to date across its repositories.
All repos share a common baseline configuration with repository-specific rules where needed.

## Shared settings

Every `renovate.json` extends `config:recommended` and sets a **7-day minimum release age**.
The cooldown period reduces exposure to broken or yanked releases — see
[Renovate's *Minimum Release Age* docs](https://docs.renovatebot.com/configuration-options/#minimumreleaseage) for details.

## Per-repository configuration

### diracx

| Dependency type                    | Manager          | Notes                                 |
| ---------------------------------- | ---------------- | ------------------------------------- |
| GitHub Actions                     | `github-actions` | Grouped into a single PR              |
| Python packages (`pyproject.toml`) | `pep621`         | Auto-detected by `config:recommended` |
| Dockerfiles                        | `dockerfile`     | Auto-detected by `config:recommended` |

### diracx-charts

| Dependency type                | Manager              | Notes                                  |
| ------------------------------ | -------------------- | -------------------------------------- |
| GitHub Actions                 | `github-actions`     | Grouped into a single PR               |
| Helm chart dependencies        | `helmv3`             | Requires Dependency Dashboard approval |
| Tool versions in `run_demo.sh` | Custom regex manager | Grouped into a single PR               |

### diracx-web

| Dependency type | Manager          | Notes                                 |
| --------------- | ---------------- | ------------------------------------- |
| GitHub Actions  | `github-actions` | Grouped into a single PR              |
| npm packages    | `npm`            | Auto-detected by `config:recommended` |

## What Renovate does *not* manage

pre-commit hooks
:   Managed by [pre-commit.ci](https://pre-commit.ci/), which opens its own update PRs.

Security alerts
:   GitHub's Dependabot **security alerts** remain enabled via repository settings.
    These are independent of Dependabot *version updates*.
