# Making and deploying releases

The versioning of `DIRAC`, `DIRACCommon`, `diracx`, `diracx-charts` and all the extensions are very linked to one another.

```mermaid
flowchart LR
    subgraph diracx-block["diracx"]
        diracx-charts("diracx-charts")
        diracx("diracx")
        diracx-web("diracx-web")
    end

    subgraph extension-block["extension"]
        extension("extension")
        extension-web("extension-web")
        extension-charts("extension-charts")
    end


    diracx -->|commit| diracx-charts
    diracx-web -->|commit| diracx-charts

    diracx -->|renovate| extension
    diracx-web -->|renovate| extension-web
    extension -->|commit and bump diracx-charts| extension-charts
    extension-web -->|commit| extension-charts

    linkStyle 0 stroke: blue
    linkStyle 1 stroke: blue
    linkStyle 2 stroke: red
    linkStyle 3 stroke: red
    linkStyle 4 stroke: blue
    linkStyle 5 stroke: blue
```

## Dependencies between diracx, DIRAC and DIRACCommon

Both `DIRAC` and `diracx` depend on `DIRACCommon`. However, `DIRACCommon` is in the same repository as `DIRAC`, and versioned together. This means that if we need to move code from `DIRAC` to `DIRACCommon` such that it is usable by `diracx`, we need first a new release of `DIRAC` and `DIRACCommon` just moving the code, before being able to use it in `diracx`.

The releases of `DIRAC` and `DIRACCommon` are managed as they have always been, and are described in the `DIRAC` documentation

## Dependencies between diracx and diracx-charts

`diracx-charts` has a version number, as well as an `appversion`, which corresponds to the `diracx` version. This means that a new release of `diracx` requires a new `diracx-charts` release, which will update:

- the diracx-charts version
- the AppVersion
- the container versions

The `update-charts` CI job in `diracx` pushes a new commit in `diracx-charts`, which results in a new `diracx-charts` release.

## Dependencies between diracx and the extensions

Because the client extension regenerates the vanilla diracx client as well, the extension MUST have a strict dependency on the `diracx` version. This dependency is at the level of the python packaging (`pyproject.toml`) as well as in the containers (`Dockerfile`).

Your extension should setup [Renovate](https://github.com/renovatebot/renovate), this way it will pick up the new `diracx` version.

In turn, a new extension release should trigger a release of your extension chart.
In this case, a new extension-chart release means updating:

- the chart version
- the AppVersion
- the containers versions
- the diracx-charts version to the latest compatible with the diracx version required by lhcbdiracx

## Dependencies between diracx, diracx-web and diracx-charts

The `diracx-web` version is only very loosely coupled with the `diracx` version.
A new `diracx-web` release will trigger a new `diracx-charts` updating:

- the chart version
- the container versions
- NOT the AppVersion

## diracx release

Releases are managed by `ReleasePlease`. The token used to perform it belongs to `diracbot` github user

[PROCESS TO BE COMPLETED BY ALEXANDRE]

A new release creates:

- a [github release](https://github.com/DIRACGrid/diracx/releases)
- a [pypi release](https://pypi.org/project/diracx/)
- new [docker images](https://github.com/DIRACGrid/diracx/)

### Force version number

You can force a specific version number following the [doc](https://github.com/googleapis/release-please?tab=readme-ov-file#how-do-i-change-the-version-number)

```bash
git commit --allow-empty -m "chore: release 0.0.3" -m "Release-As: v0.0.3"
git push upstream HEAD:main
```

## diracx-charts releases

Every commit in master triggers a new release. The releases are published on a [helm repo](https://charts.diracgrid.org/index.yaml)
