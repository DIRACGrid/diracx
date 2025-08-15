# Extensions

DiracX supports the concept of extensions to modify and extend it's functionality to meet community specific needs.
This is typically done by larger installations that serve a single virtual organization and for whom community specific workflows are well established.
While DiracX extensions are a powerful and flexible tool, they also require ongoing maintenance to follow upstream DiracX changes.
Before embarking on this journey, consider contributing to DiracX itself and avoiding all of the long term maintenance costs.

## Do you need an extension?

Before going any further you should consider if you even need a DiracX extension.
Creating a standard python package which uses the [DiracX Python API](../../user/reference/programmatic-usage/python-interface.md) might be a better choice as this will be more stable over time and leave you with more flexibility in how you implement your code.
For example, if you wish to create a job submission framework which let's people submit specialized jobs, monitor their status and then download data you're likely best off not making a DiracX extension.

Alternatively, you should make an extension if you want to:

- Add a custom metadata and province catalog with deep integration into the transformation system based on the specific workflows if your community.
- Interact directly with the DiracX databases for automating installation specific operational tasks.
- Customize the authorization logic

Lastly, in some situations it might make sense to contribute to DiracX itself rather than having an extension if the changes are small or potentially useful to other installations.

As always, [open a discussion](https://github.com/DIRACGrid/diracx/discussions) or attend the [biweekly DIRAC community meeting](https://indico.cern.ch/category/4205/) if you are unsure and would like to discuss your needs.

## What do you need to extend?

Okay, so you've decided you definitely do need an extension but not all extensions are made equal.
Before proceeding you should read and and understand the [structure of DiracX](./components/index.md) to know which components you want to extend.
For most components you can extend them in isolation without needing to have an extension for the other components, for example:

- `diracx-web` can be extended to customize the web interface without needing to extend any of the Python packages.
- `diracx-logic` can be extended without needing to extend `diracx-core` or `diracx-routers`

The exception to this is when extending `diracx-routers` you MUST also extend `diracx-client` and your client extension MUST be regenerated for every DiracX release.
More details about this can be found in the [dedicated how to](../how-to/client-extension.md).

## Gubbins

`gubbins` is a `diracx` extension that serves as show-case for everything which is possible to extend.

It should also serve as a reference doc on how to write your own extension. Everything in the `diracx` dev documentation applies here too.
If you write your own extension, just replace `gubbins` with whatever name you chose (e.g. `lhcbdiracx`, `belle2diracx`, `donotforgetgriddpp`).
The structure of the repo, the content of the `pyproject.toml` files, the `__init__.py` of the modules... are all skeletons that you must reproduce.
It is not required to reproduce all submodules (e.g. you can have `myextension-cli` without having any other components).
**Any use cases not included in `gubbins` are not supported.**
If you think you need additional functionality please open an issue to discuss so it can be added here to assist with long term stability.

Most functionality is managed via entrypoints in the various `pyproject.toml` files.
The only essential one to have is:

```toml
[project.entry-points."diracx"]
extension = "myextension"
```

We recommend putting this in `myextension-core` however it is possible to include it any of your packages (e.g. `myextension-cli` if you only have a CLI extension).
It is also acceptable to include it in multiple packages.

To find out more about the entrypoints available for extensions, see [here](../reference/entrypoints.md).

!!! warning

    As `gubbins` is hosted in the main DiracX repository there are a couple of things that would need to be changed for a standard extension:

    - `root = "../../.."` in `pyproject.toml` should be `root = ".."` (i.e. the path to the root of your repository)
    - The GitHub actions file `.github/workflows/extensions.yaml` should in fact be split in multiple jobs under `.github/workflows/` of your repo.

## Installing the extension

To develop, follow the same [instruction as `diracx`](../tutorials/getting-started.md).

## `gubbins-db`

The `gubbins-db` package contains the extension for the DB.

### New DB

`lollygag` is a DB which is specific to `gubbins`, i.e. it does not modify or extend an existing `diracx` db

### Extended DB

`GubbinsJobDB` illustrates how to extend an existing `diracx` DB, add new methods, modify methods, add a table.

A router test exists (`test_gubbins_job_manager.py`), even though no router is redefined. It is just to show that the correct DB is being loaded.

!!! warning

    In the test dependency, you need to specify both the original DiracX `JobDB` as well as the extended one `GubbinsJobDB`. To avoid that inconvenience, reuse the same name (i.e. `JobDB` instead of `GubbinsJobDB`).

## `gubbins-routers`

The `gubbins-routers` package contains the extension for the routers.

### New router

`lollygag` is a router which is specific to `gubbins`, i.e. it does not modify or extend an existing `diracx` routers. It uses the `lollygagDB`. It also makes use of gubbins' specific `properties` and `AccessPolicy`

### Existing router

`well-known` overwrites the `dirac-metadata` endpoint. It also changes the return type and makes use of gubbins' specific configs.

## `gubbins-client`

TODO This probably should be moved to [here](../how-to/client-extension.md).

The requirements are the following:

- Working with the `DiracClient` should allow you to call the API from the extension
- It should be possible to use directly the extension client (i.e. `GubbinsClient`)
- Methods/Operations/models that are patched in `diracx` cannot be re-patched in the extension

### New client

To create a client extension:

- mirror the structure of the `diracx-client`
- Generate a client in `generated` using `Autorest` For this the best is to have a temporary router test writing the `openapi.json` somewhere

```python
r = normal_user_client.get("/api/openapi.json")
with open("/tmp/openapi.json", "wt") as f:
    json.dump(r.json(), f, indent=2)
```

- The autorest command then looks something like

```bash
autorest --python --input-file=/tmp/openapi.json --models-mode=msrest --namespace=generated --output-folder=gubbins-client/src/gubbins/
```

- Create the `patches` directory, simply exporting the generated `clients` (both sync and async)
- Define the base modules to export what is needed
- The top init file MUST have

```python
import diracx.client
```

- Generate the autorest client (see CI `regenerate_client`)

## `gubbins-cli`

The following CLI extensions are supported:

- add a new subcommand
- extend an existing subcommand
- modify an existing subcommand
- no `gubbins` CI, everything through `dirac` cli

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

## `gubbins-core`

### Configuration

Only extending the configuration is allowed. For example, you can add extra fields for the users

You need to:

- Redefine a new configuration schema
- Declare this new class in the `diracx` entrypoint

```toml
[project.entry-points."diracx"]
config = "gubbins.core.config.schema:Config"
```

- Redefine a dependency for your routers to use

### Properties

Properties can only be added. This is done in the `gubbins-core` `pyproject.toml`

```toml
[project.entry-points."diracx"]
properties_module = "gubbins.core.properties"
```

The gubbins properties module illustrates how to do it

## `gubbins-testing`

`diracx-testing` package contains a lot of useful tools for testing `diracx` and its extensions.

Note that even if you have your own `testing` package depending on `diracx-testing`, you should specify it when calling `pytest` (see various `pyprojects.toml`)
