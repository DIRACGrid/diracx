Where we want to go, not where we are

* each package runs unit tests in different jobs to ensure that there is no hidden dependencies: pytest and mypy
* run the integration tests (against the demo) in a single job

For the unit test, we start with a crude conda environment, and do pip install of the package.

For the integration tests, we always use the [services|tasks|client]-base image and do  apip install directly.

Same for unit tests (router tests use `servces-base`, etc)
