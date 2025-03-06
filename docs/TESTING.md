What we do

* each package runs unit tests in different jobs to ensure that there is no hidden dependencies: pytest and mypy
* run the integration tests (against the demo) in a single job

For the unit test, we start with a crude conda environment, and do pip install of the package.
Note: `diracx-logic` does not contain any unit tests, developers are expected to run tests from `diracx-routers`.

For the integration tests, we always use the [services|tasks|client] dev image and do a pip install directly with ``--no-deps``.

Same for unit tests (router tests use `services:dev`, etc)
