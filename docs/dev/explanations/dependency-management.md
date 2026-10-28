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
| `pixi.lock`                        | `pixi`           | Lock file maintenance in a single PR  |

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

### Changing `pixi.lock` in other pull requests

Renovate's lock file maintenance PR is the normal way for `pixi.lock` to change.
Avoid running `pixi update` in other pull requests, as it re-resolves every environment and hides the change you meant to make among hundreds of unrelated ones.

When a pull request does need to change dependencies, edit `pixi.toml` or the relevant `pyproject.toml` and run `pixi lock`, which only changes what the edit requires.
Then check the result against `main`:

```bash
pixi run lock-diff                        # compares with origin/main
pixi run lock-diff --base upstream/main   # if origin is your fork
```

The same summary is posted as a comment on every pull request that changes `pixi.lock`, and reviewers should use it rather than the raw diff.
The "Pixi Lock" check fails if a pull request downgrades any package, unless it comes from Renovate.

`pixi.lock` is marked `merge=binary` in `.gitattributes`, so if `main` changes it while a pull request is open, git reports a conflict instead of trying to combine the two.
Don't resolve the conflict by keeping the branch's version: it still satisfies `pixi.toml`, so `pixi lock` leaves it alone and the merge would roll back everything Renovate updated on `main`.
Take the lock file from `main` and re-lock instead:

```bash
git checkout origin/main -- pixi.lock
pixi lock
```

If a downgrade or a lock-only change is deliberate, explain why in the pull request description and ask a maintainer to add the `deps: intentional-lock-change` label.

## What Renovate does *not* manage

pre-commit hooks
:   Managed by [pre-commit.ci](https://pre-commit.ci/), which opens its own update PRs.

Security alerts
:   GitHub's Dependabot **security alerts** remain enabled via repository settings.
    These are independent of Dependabot *version updates*.
