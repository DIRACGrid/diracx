# Changelog

## [0.0.8](https://github.com/DIRACGrid/diracx/compare/v0.0.7...v0.0.8) (2026-02-02)


### Features

* allow git commit hash usage in config source revision ([#726](https://github.com/DIRACGrid/diracx/issues/726)) ([8b8b687](https://github.com/DIRACGrid/diracx/commit/8b8b687dc7a72a5a3bb904a338969351e64e6173))
* disallow relative paths with any depth other than zero ([82e7d15](https://github.com/DIRACGrid/diracx/commit/82e7d15effdf3b7d6807b9d287ca7f0549402f6e))


### Bug Fixes

* add ci skip settings-doc-check ([ccb1f48](https://github.com/DIRACGrid/diracx/commit/ccb1f489902a45ceea2524ec3213968641db5e2e))
* added a StrEnum for Entrypoints to avoid spelling mistakes ([#744](https://github.com/DIRACGrid/diracx/issues/744)) ([19bfadc](https://github.com/DIRACGrid/diracx/commit/19bfadc1c473626dfe9f3a6489248c098056186e))
* annoying wrapper script ([ccb1f48](https://github.com/DIRACGrid/diracx/commit/ccb1f489902a45ceea2524ec3213968641db5e2e))
* cleanups ([ccb1f48](https://github.com/DIRACGrid/diracx/commit/ccb1f489902a45ceea2524ec3213968641db5e2e))
* cs conversion takes into account all the parent classes ([#753](https://github.com/DIRACGrid/diracx/issues/753)) ([b742a34](https://github.com/DIRACGrid/diracx/commit/b742a34150d9d925460de4db2c5eba0cb5eafd49))
* format ([0440a9c](https://github.com/DIRACGrid/diracx/commit/0440a9cecba1b2302630b5aae707b6162954332d))
* format settings doc files ([ccb1f48](https://github.com/DIRACGrid/diracx/commit/ccb1f489902a45ceea2524ec3213968641db5e2e))
* formatting ([0440a9c](https://github.com/DIRACGrid/diracx/commit/0440a9cecba1b2302630b5aae707b6162954332d))
* formatting and remove caching ([4ed2709](https://github.com/DIRACGrid/diracx/commit/4ed2709980338cb0e423ef5630bdf6796e4c7250))
* improve pytest-integration debugging output ([#740](https://github.com/DIRACGrid/diracx/issues/740)) ([d691175](https://github.com/DIRACGrid/diracx/commit/d691175438db532bd833ceecf1ad36ea3de29a5d))
* reorder imports ([4ed2709](https://github.com/DIRACGrid/diracx/commit/4ed2709980338cb0e423ef5630bdf6796e4c7250))
* set exclude=True on computed field ([4ed2709](https://github.com/DIRACGrid/diracx/commit/4ed2709980338cb0e423ef5630bdf6796e4c7250))
* skip CI on release-please PRs ([#759](https://github.com/DIRACGrid/diracx/issues/759)) ([f236f61](https://github.com/DIRACGrid/diracx/commit/f236f6185db30c0a7faa7fce7f280697484087eb)), closes [#685](https://github.com/DIRACGrid/diracx/issues/685)
* update tasks ([ccb1f48](https://github.com/DIRACGrid/diracx/commit/ccb1f489902a45ceea2524ec3213968641db5e2e))
* use batched DB deletes with SKIP LOCKED for sandbox cleanup ([#739](https://github.com/DIRACGrid/diracx/issues/739)) ([584d4e6](https://github.com/DIRACGrid/diracx/commit/584d4e62a87d1b2ffbf739e80fe44f9c0fb92c9a))
* well that didn't work ([ccb1f48](https://github.com/DIRACGrid/diracx/commit/ccb1f489902a45ceea2524ec3213968641db5e2e))


### Miscellaneous Chores

* release 0.0.8 ([80f1a37](https://github.com/DIRACGrid/diracx/commit/80f1a376d8e9e51fab4f6b45d1e5bf66e73f8d4e))

## [0.0.7](https://github.com/DIRACGrid/diracx/compare/v0.0.6...v0.0.7) (2026-01-20)


### Bug Fixes

* add logging and retry for cache population failures ([#736](https://github.com/DIRACGrid/diracx/issues/736)) ([b899a8d](https://github.com/DIRACGrid/diracx/commit/b899a8d207a42b232fbe4062f329a32d64c37ec3))
* require fastapi&gt;=0.121.0 for Depends scope parameter ([#737](https://github.com/DIRACGrid/diracx/issues/737)) ([fb15f9c](https://github.com/DIRACGrid/diracx/commit/fb15f9c5bb9d36568e37f0fae7a2a78995f02d51))

## [0.0.6](https://github.com/DIRACGrid/diracx/compare/v0.0.5...v0.0.6) (2026-01-20)


### Bug Fixes

* use correct argument name for update_chart_version.py ([84390f0](https://github.com/DIRACGrid/diracx/commit/84390f0ec66bae4db342864da52810b26955c4bd))

## [0.0.5](https://github.com/DIRACGrid/diracx/compare/v0.0.4...v0.0.5) (2026-01-20)


### Features

* add logging for error conditions and edge cases ([#734](https://github.com/DIRACGrid/diracx/issues/734)) ([2c8b5c6](https://github.com/DIRACGrid/diracx/commit/2c8b5c682f17270e341deaf23cd4a7b4b7c4dd0a))
* extend update_chart_version.py for downstream charts ([#731](https://github.com/DIRACGrid/diracx/issues/731)) ([b1a293b](https://github.com/DIRACGrid/diracx/commit/b1a293b01ad062d162afdfb5377a76c95eb09913))


### Bug Fixes

* use YAML parser for chart version to handle comments ([9516971](https://github.com/DIRACGrid/diracx/commit/95169716206bb476f450fff8fd228ada649fef5b))


### Miscellaneous Chores

* release 0.0.5 ([c0f166b](https://github.com/DIRACGrid/diracx/commit/c0f166bf29e1ed0ac2b5aa89ebb39ac60cfe23d3))

## [0.0.4](https://github.com/DIRACGrid/diracx/compare/v0.0.3...v0.0.4) (2026-01-19)


### Bug Fixes

* use extension-aware Config discovery in testing fixture ([#727](https://github.com/DIRACGrid/diracx/issues/727)) ([49d0fb8](https://github.com/DIRACGrid/diracx/commit/49d0fb8c2718a5668f330219025832365119d15d))

## [0.0.3](https://github.com/DIRACGrid/diracx/compare/v0.0.2...v0.0.3) (2026-01-19)


### Features

* add gubbins-charts Helm chart for gubbins extension ([#691](https://github.com/DIRACGrid/diracx/issues/691)) ([649f318](https://github.com/DIRACGrid/diracx/commit/649f318daa8eeaec177cfde4bc2830a2a89a9058))


### Bug Fixes

* do not use Any from pyparsing ([#725](https://github.com/DIRACGrid/diracx/issues/725)) ([b3873a1](https://github.com/DIRACGrid/diracx/commit/b3873a1d8859b670ac61e4d124a91a38f2e9c807))
* ensure database commits complete before HTTP responses ([#722](https://github.com/DIRACGrid/diracx/issues/722)) ([4685790](https://github.com/DIRACGrid/diracx/commit/46857909b9259c13e8603b2cbaecb212d3229784))
* exclude CHANGELOG.md from mdformat ([639026a](https://github.com/DIRACGrid/diracx/commit/639026a19605afd8ae94c67f36bd90b5c381b45b))
* move cern-specific config option from diracx to lhcbdiracx ([#694](https://github.com/DIRACGrid/diracx/issues/694)) ([927d51b](https://github.com/DIRACGrid/diracx/commit/927d51bb8cf21b5e4f82c53b7e04bf0e72435806))
* support additional config hints ([#690](https://github.com/DIRACGrid/diracx/issues/690)) ([c265a47](https://github.com/DIRACGrid/diracx/commit/c265a47363850662502bb7f0fb8ebbbe94893e85))


### Miscellaneous Chores

* release 0.0.3 ([dbc726c](https://github.com/DIRACGrid/diracx/commit/dbc726c2e6ac1ed5555b8144b01fea71b5e897ba))

## [0.0.2](https://github.com/DIRACGrid/diracx/compare/v0.0.1...v0.0.2) (2025-10-22)

### Bug Fixes

- correct release-please integration in deployment workflow ([#683](https://github.com/DIRACGrid/diracx/issues/683)) ([0084328](https://github.com/DIRACGrid/diracx/commit/00843286b49a2a075226ad47af746d03b9413d60))

## [0.0.1](https://github.com/DIRACGrid/diracx/compare/v0.0.1...v0.0.1) (2025-10-22)

### Features

- DiracX is here! This is the companion release of DIRAC v9 and the first non-prerelease version of DiracX.

### Miscellaneous Chores

- release 0.0.1 ([615014d](https://github.com/DIRACGrid/diracx/commit/615014dcf87d985c7b286b1c8d94e4c4520d8463))
