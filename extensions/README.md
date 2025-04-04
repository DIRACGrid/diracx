
# Gubbins

`gubbins` is a `diracx` extension. It is a show-case for everything which is possible to extend.

It should also serve as a reference doc on how to write your own extension. Everything in the `diracx` dev documentation applies here too.
If you write your own extension, just replace `gubbins` with whatever name you chose (e.g. `lhcbdiracx`, `belle2diracx`, `donotforgetgriddpp`).
The structure of the repo, the content of the `pyproject.toml` files, the `__init__` of the modules... are all skeletons that you must reproduce.
It is not required to reproduce all submodules (e.g. you can have `myextension-cli` without having any other components).

Most functionality is managed via entrypoints in the various `pyproject.toml` files.
The only essential one to have is:

```toml
[project.entry-points."diracx"]
extension = "myextension"
```

We recommend putting this in `myextension-core` however it is possible to include it any of your packages.
It is also acceptably to include it in multiple packages.

This here is the exhaustive list of what is supported and tested for extensions.
Any use cases not listed here are not supported, if you think you need additional functionality please open an issue to discuss so it can be added here to assist with long term stability.

NOTE: This documentation is still a work in progress!!!


## QUESTIONS

What to do with the `environment.yaml` ? should we replicate wht's in diracx ?


## General statements

The fact of having `gubbins` as a subfolder has a few downside which you will not suffer if having your extension in a separate repository:
* the `root` of `setuptools_scm` in the various `pyproject.toml` will only be `..` for your extension

## CI

The extension is tested in the CI.

What is in the [action file](``.github/workflows/extensions.yaml``) should in fact be split in multiple jobs under ``.github/workflows/`` of your repo.

Here we use the local versions of `diracx` packages to make sure we are always up to date, and that a change in `diracx` does not break the extension mechanisms. But in your real extension, you will want to depend on published package (i.e. ``pip install diracx-routers`` instead of ``pip install -e ./diracx-routers`` for example), and on published docker images.

Moreover, the `gubbins` docker images are never uploaded, and are only passed from on job to the next. You should definitely upload yours.

## Installing the extension

To develop, follow the same instruction as `diracx`

```bash
# Create the mamba environment
mamba env create --file extensions/gubbins/environment.yml
conda activate gubbins-dev


# Make an editable installation of diracx
pip install -r requirements-dev.txt

# Make an editable installation of gubbins
cd extensions/gubbins/
pip install -r requirements-dev.txt

# Install the patched DIRAC version
pip install git+https://github.com/DIRACGrid/DIRAC.git@integration

# Enable pre-commit
mamba install pre-commit
pre-commit install
```

## DB

The `gubbins-db` package contains the extension for the DB.

### New DB

`lollygag` is a DB which is specific to `gubbins`, i.e. it does not modify or extend an existing `diracx` db

### Extended DB

`GubbinsJobDB` illustrates how to extend an existing `diracx` DB, add new methods, modify methods, add a table.

A [router test](extensions/gubbins/gubbins-routers/tests/test_gubbins_job_manager.py) exists, even though no router is redefined. It is just to show that the correct DB is being loaded.

> [!WARNING]
> in the [test dependency](gubbins/gubbins-routers/tests/test_gubbins_job_manager.py), you need to specify both the original DiracX `JobDB` as well as the extended one `GubbinsJobDB`. To avoid that inconvenience, reuse the same name (i.e. `JobDB` instead of `GubbinsJobDB`)


## Routers

The `gubbins-router` package contains the extension for the DB.


### New router

`lollygag` is a router which is specific to `gubbins`, i.e. it does not modify or extend an existing `diracx` routers. It uses the `lollygagDB`. It also makes use of gubbins' specific `properties` and `AccessPolicy`

### Existing router

`well-known` overwrites the `dirac-metadata` endpoint. It also changes the return type and makes use of gubbins' specific configs.


## Client

The requirements are the following:

* Working with the `DiracClient` should allow you to call the API from the extension
* It should be possible to use directly the extension client (i.e. `GubbinsClient`)
* Methods/Operations/models that are patched in `diracx` cannot be re-patched in the extension


### New client

To create a client extension:
* mirror the structure of the `diracx-client`
* Generate a client in `generated` using `Autorest` For this the best is to have a temporary router test writing the `openapi.json` somewhere
```python
r = normal_user_client.get("/api/openapi.json")
with open('/tmp/openapi.json', 'wt') as f:
    json.dump(r.json(), f, indent=2)
```
* The autorest command then looks something like
```bash
autorest --python --input-file=/tmp/openapi.json --models-mode=msrest --namespace=generated --output-folder=gubbins-client/src/gubbins/
```

* Create the `patches` directory, simply exporting the generated `clients`(both [sync](gubbins/gubbins-client/src/gubbins/client/patches/__init__.py) and [async](gubbins/gubbins-client/src/gubbins/client/patches/aio/__init__.py))
* Define the base modules to export what is needed
* The [top init](gubbins/gubbins-client/src/gubbins/client/__init__.py) MUST have
```python
import diracx.client
```

* Generate the autorest client (see CI `regenerate_client`)

## CLI

The following CLI extensions are supported:
* add a new subcommand
* extend an existing subcommand
* modify an existing subcommand
* no `gubbins` CI, everything through `dirac` cli

The CLI is managed by the `diracx.cli` entry point

```toml
[project.entry-points."diracx.cli"]
jobs = "diracx.cli.jobs:app"
config = "diracx.cli.config:app"

[project.entry-points."diracx.cli.hidden"]
internal = "diracx.cli.internal:app"
```

See the `gubbins-cli` package for instructions

### New subcommand


`lollygag` is a new CLI command, calling the `lollygag` router.

### Changing a subcommand

For completely replacing a subcommand, it is enough to change the corresponding entrypoint in the `pyproject.toml`

### Extending a subcommand

You can modify the behavior of a specific CLI call, or add extra calls to an existing subcommand. The `config` CLI shows just that.


## Configuration

Only extending the configuration is allowed. For example, you can add extra fields for the users

You need to:

* Redefine a new configuration [schema](gubbins/gubbins-core/src/gubbins/core/config/schema.py)
* Declare this new class in the `diracx` entrypoint
```toml
[project.entry-points."diracx"]
config = "gubbins.core.config.schema:Config"
```
* Redefine a dependency for your routers to use (see [example](gubbins/gubbins-routers/src/gubbins/routers/dependencies.py))


## Properties

Properties can only be added. This is done in the `gubbins-core` `pyproject.toml`


```toml
[project.entry-points."diracx"]
properties_module = "gubbins.core.properties"
```

[properties](gubbins/gubbins-core/src/gubbins/core/properties.py) illustrates how to do it


## Writing tests

`diracx-testing` package contains a lot of useful tools for testing `diracx` and its extensions.

Note that even if you have your own `testing` package depending on `diracx-testing`, you should specify it when calling `pytest` (see various `pyprojects.toml`)


## Work on gubbins

Working on this test extension is tricky as it is a subdirectory. if you want to debug it you need to:

* buid the `gubbins` docker images (or download an art)
* copy the files somewhere else
* edit the pyproject
* run_demo

This is what is done in the [CI](../.github/workflows/extensions.yml)

## Deploying on kubernetes

The CI comes with a specific [`values.yaml`](gubbins_values.yaml).
