# Changelog

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
