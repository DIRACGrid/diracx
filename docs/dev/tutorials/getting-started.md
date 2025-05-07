TODO
GEtting a dev environment
`run_local`
`run pytest`
`run_demo` -> link diracx-charts

How we approach testing
unit test as much as we can, should be fast

Integration testing against the demo

## Create the dev environment

This will help you setup a dev environment to run the unit tests

Requirements: conda, git

```bash
# Clone the diracx repository
git clone git@github.com:DIRACGrid/diracx.git
cd diracx

# Create the mamba environment
mamba env create --file environment.yml
conda activate diracx-dev

# Make an editable installation of diracx

pip install -r requirements-dev.txt

# Install the patched DIRAC version
pip install git+https://github.com/DIRACGrid/DIRAC.git@integration

# Enable pre-commit
mamba install pre-commit
pre-commit install
```

### Run the test

Run the unit tests:

```bash
# In the `diracx` folder
pytest
mypy
pre-commit run --all-files
```

Some tests require the DiracX demo instance to be running (see above) and are skipped by default.
To enable these tests pass `--demo-dir` like so:

```bash
pytest --demo-dir=../diracx-charts/
```

### Run a local instance of diracx

This only runs the diracx server, not any dependency like external IdP.

```bash
# In the `diracx` folder
./run_local.sh
```
