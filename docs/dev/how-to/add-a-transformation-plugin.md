## Add a feeder or packer

Feeders and packers are the plugins that shape a transformation's input pool ([DX-ADR-006](../../adr/DX-ADR-006_extensions.md)). A **feeder** finds new inputs, for example by generating seeds or querying a file catalogue. A **packer** groups the transformation's `Unassigned` inputs into parcels. Both follow one rule: **plugins propose, the core writes.** A plugin yields descriptions of what should happen, and only the core turns them into database rows.

This guide describes the contract as implemented. It extends the sketches in DX-ADR-006, where plugins are synchronous generators and injected parameters have no default, with async generators and the `INJECTED` marker.

This guide shows how to write, register, type-check and test either kind. The contracts live in `diracx.core.models.transformation`, so a plugin in an extension such as `gubbins` only needs `diracx-core` to be type-checked.

### What a plugin receives

A plugin is an async generator function that takes keyword-only parameters of three kinds. They differ in who decides the value and how often it changes:

| Kind                      | Decided by                              | Varies per     | Stored in                                                                                                    | Examples                                                       |
| ------------------------- | --------------------------------------- | -------------- | ------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------- |
| **Contract parameters**   | the core, at each call                  | invocation     | the transformation's rows (`FeederState`, `Inputs`, `FeederEnabled`), or an operator's instruction (`flush`) | `feeder_state`, `scouting`; `inputs`, `flush`, `feeder_active` |
| **Arguments model**       | the workgraph submitter, then hooks     | transformation | the `FeederArgs` / `PackerArgs` column                                                                       | a seed count, a dataset name, a group size                     |
| **Injected dependencies** | the DiracX administrator, at deployment | installation   | settings (environment variables), DBs                                                                        | a catalogue's URL and credentials, a database                  |

The set of contract parameters is fixed per kind: every feeder receives the same ones, and so does every packer. The core builds them at each call, from what it has stored for the transformation (its bookmark, its `Unassigned` inputs, whether its feeder is enabled), from counts and states it derives (`input_overview`, `scouting`), or from a one-off instruction that is not stored at all (`flush`).

The fields of the arguments model belong to the plugin: the seed feeder below defines `max_count` in its `SeedFeederArgs`. The values belong to each transformation: two transformations using that feeder store their own `max_count`, and hooks may change it while the transformation runs. The plugin itself is a plain function shared by every transformation that uses it.

When deciding where a value belongs, ask who should be able to change it:

- A value the core already knows at call time is a **contract parameter**. The contract is fixed, so you cannot add one.
- A value that differs between transformations goes in the **arguments model**. Users set it when they submit, so it must be safe for them to choose.
- A value that is the same for every transformation in an installation is an **injected dependency**. A catalogue URL is one: storing it per transformation would mean rewriting every row when the service moves, and it would let any submitter point the plugin at a server of their choice. If a transformation must choose between a few instances of a service, put the instance *name* in the arguments model and keep the mapping from name to URL in settings.

### Define the arguments model

Subclass `PluginArgs`. It is a pydantic model that rejects unknown keys, so a misspelt argument fails validation instead of silently falling back to a default, and its instances are immutable, so a plugin cannot change the arguments it was given. Document each field with a docstring below it.

```python
from pydantic import NonNegativeInt, model_validator

from diracx.core.models.transformation import PluginArgs


class SeedFeederArgs(PluginArgs):
    max_count: NonNegativeInt = 0
    """Number of seeds to issue."""
    scouting_count: NonNegativeInt = 10
    """Number of seeds to issue while the workgraph is scouting."""

    @model_validator(mode="after")
    def _scout_is_a_sample(self) -> "SeedFeederArgs":
        if self.scouting_count > self.max_count:
            raise ValueError("scouting_count cannot exceed max_count")
        return self
```

Use a `model_validator` for rules that span several fields. The core validates the stored arguments against this model whenever they are written or read, so these checks run at submission rather than at the first sweep.

### Write a feeder

A feeder yields three kinds of proposal:

- `FeederInput` is one input. Give it an `lfn` and `lfn_size` for a file, or only a `descriptor` for anything else, such as a seed. The core inserts it as `Unassigned`, and drops it if the transformation already has an input with the same `(lfn, descriptor)`. Yielding an input twice is therefore harmless.
- `SetFeederState` stores a new bookmark. It commits together with the inputs yielded before it, so yield it after each batch to make the feeder restartable from that point.
- `NoMoreFeederInputs` signals that the source is exhausted. The core disables the feeder and stops reading from the generator.

`feeder_state` is the last bookmark the feeder stored. It is read-only: to change it, yield a `SetFeederState`. Its content is whatever JSON the feeder stored, so check its shape before using it.

```python
from collections.abc import AsyncGenerator, Mapping

from pydantic import JsonValue

from diracx.core.models.transformation import (
    FeederInput,
    FeederProposal,
    NoMoreFeederInputs,
    SetFeederState,
)


async def seed_feeder(
    *,
    feeder_args: SeedFeederArgs,
    feeder_state: Mapping[str, JsonValue],
    scouting: bool,
) -> AsyncGenerator[FeederProposal, None]:
    next_seed = feeder_state.get("next_seed", 0)
    if not isinstance(next_seed, int):
        raise ValueError(f"Unexpected bookmark: {feeder_state!r}")
    target = feeder_args.scouting_count if scouting else feeder_args.max_count

    for seed in range(next_seed, target):
        yield FeederInput(descriptor={"seed": seed})
    yield SetFeederState(state={"next_seed": max(next_seed, target)})
    if not scouting:
        # Every seed up to max_count is issued: nothing more will come
        yield NoMoreFeederInputs()
```

Keep branching on `scouting` minimal, so that what runs during scouting stays representative of what runs afterwards.

### Write a packer

A packer is offered the transformation's claimed `Unassigned` inputs as an async iterator of `PackerInput`, and yields:

- `ComputeParcel` or `DataParcel`, a parcel made of some of those inputs;
- `BadInput`, an input that can never be used, which the core moves to `Problematic` with the given reason;
- `DelayInput`, inputs to offer again only after a given time.

Inputs the packer neither packs nor rejects stay `Unassigned` and are offered again on the next run. `flush` and `feeder_active` tell the packer when it may stop waiting for more inputs, and `input_overview` counts the transformation's inputs per status. This is the `FixedSize` packer shipped with DiracX:

```python
from collections.abc import AsyncGenerator, AsyncIterator, Mapping
from uuid import UUID

from pydantic import PositiveInt

from diracx.core.models.transformation import (
    ComputeParcel,
    InputStatus,
    PackerInput,
    PluginArgs,
)


class FixedSizePackerArgs(PluginArgs):
    group_size: PositiveInt = 1
    """Number of inputs in each parcel."""


async def fixed_size_packer(
    *,
    packer_args: FixedSizePackerArgs,
    inputs: AsyncIterator[PackerInput],
    flush: bool,
    feeder_active: bool,
    input_overview: Mapping[InputStatus, int],
) -> AsyncGenerator[ComputeParcel, None]:
    group: list[UUID] = []
    async for packer_input in inputs:
        group.append(packer_input.input_id)
        if len(group) == packer_args.group_size:
            yield ComputeParcel(input_ids=group)
            group = []
    # Hold back a short last group while more inputs may still arrive
    if group and (flush or not feeder_active):
        yield ComputeParcel(input_ids=group)
```

A plugin must accept every contract parameter, even one it does not use, as `fixed_size_packer` does with `input_overview`.

### Ask for installation dependencies

This is *dependency injection*: the plugin only declares what it needs, and the framework creates the object and hands it over, so the plugin never reads environment variables or opens connections itself. DiracX already works this way for routes and tasks, where a parameter annotated with a database class receives a database.

Declare each dependency as an extra keyword parameter, annotated with its class and defaulting to `INJECTED`. When the core runs the plugin, it passes an instance of that class in place of the marker.

```python
from pydantic import AnyHttpUrl

from diracx.core.models.transformation import INJECTED
from diracx.core.settings import ServiceSettingsBase


class CatalogueSettings(ServiceSettingsBase):
    model_config = ServiceSettingsBase.model_config | {
        "env_prefix": "GUBBINS_CATALOGUE_",
        "use_attribute_docstrings": True,
    }

    url: AnyHttpUrl
    """Base URL of the file catalogue."""


class CatalogueFeederArgs(PluginArgs):
    dataset: str
    """Dataset whose files become inputs."""


async def catalogue_feeder(
    *,
    feeder_args: CatalogueFeederArgs,
    feeder_state: Mapping[str, JsonValue],
    scouting: bool,
    catalogue: CatalogueSettings = INJECTED,
) -> AsyncGenerator[FeederProposal, None]:
    # list_files is the extension's own client for the catalogue
    async for file in list_files(catalogue.url, feeder_args.dataset):
        yield FeederInput(lfn=file.lfn, lfn_size=file.size)
    yield SetFeederState(state={})
```

#### Why a marker default

`INJECTED` is a placeholder object, nothing more. It exists because of how the contract is checked:

- The `Feeder` and `Packer` Protocols list only the contract parameters, and a function matches a Protocol only if it can be called with exactly those arguments. Every extra parameter therefore needs a default. Without one, mypy reports that the plugin does not match its contract, and `load_plugin()` refuses to load it.
- An ordinary default such as `None` would also satisfy that rule, but the annotation would become `CatalogueSettings | None` and the plugin would have to handle a dependency that is missing. `INJECTED` is typed `Any`, so mypy accepts it as the default of a parameter of any type, and the annotation stays exactly the class the plugin needs.
- The marker tells the core which parameters to fill. `injected_parameters()` in `diracx.logic.transformations` returns each parameter whose default is `INJECTED`, with its annotated class, and the core passes an instance of each class by name.

#### What can be injected

Injected parameters are resolved like the dependencies of a task ([Add a task](add-a-task.md)): settings classes deriving from `ServiceSettingsBase`, which read their values from environment variables, and database classes. An injected database is for reading what the plugin needs, such as an experiment's bookkeeping. Plugins still never write to the Transformation System's tables: they yield proposals and the core writes.

!!! note

    Passing injected parameters from the tasks that run feeders and packers is still being implemented. Until then, a plugin with an `INJECTED` parameter loads, but receives the marker itself.

#### Rules for injected parameters

- Make it keyword-only (after the `*`) and annotate it with the class itself. `load_plugin()` rejects an `INJECTED` parameter whose annotation is not a class, which includes `CatalogueSettings | None`.
- Use `INJECTED` only for what the installation provides. A value that differs between transformations belongs in the arguments model, and an optional extra parameter the core knows nothing about takes an ordinary default.
- In tests, pass the dependency yourself. The marker is only a default, so a value you pass replaces it:

```python
proposals = [
    proposal
    async for proposal in catalogue_feeder(
        feeder_args=CatalogueFeederArgs(dataset="/Sim/2026"),
        feeder_state={},
        scouting=False,
        catalogue=CatalogueSettings(url="https://catalogue.example.org"),
    )
]
```

### Register the plugin

Add the function to the entry-point group of its kind in your package's `pyproject.toml`. The entry-point name is what a transformation stores in its `Feeder` or `Packer` column:

```toml
[project.entry-points."diracx.transformation.feeders"]
Catalogue = "gubbins.logic.transformations.feeders:catalogue_feeder"

[project.entry-points."diracx.transformation.packers"]
FixedSize = "diracx.logic.transformations.packers:fixed_size_packer"
```

If an extension registers a name that DiracX also registers, the extension's plugin wins.

### Check the plugin against its contract

`Feeder` and `Packer` are `typing.Protocol`s. A plugin does not inherit from them: it only has to have a matching signature, and mypy checks that wherever the plugin is assigned to a name typed with the Protocol. Add that assignment next to the plugin, in the source rather than the tests, since mypy does not check the `tests/` directories:

```python
from typing import TYPE_CHECKING

from diracx.core.models.transformation import Packer

if TYPE_CHECKING:
    # Only mypy reads this: it fails if the packer stops matching the contract
    _: Packer[FixedSizePackerArgs] = fixed_size_packer
```

mypy then rejects the plugin if it misses a contract parameter, gives one a narrower type (for example `dict` where the contract passes a `Mapping`), requires a parameter the core never passes, or uses an arguments model that is not a `PluginArgs`.

The core cannot rely on mypy, since plugins arrive through entry points. `load_plugin()` repeats the same checks when it loads a plugin, and raises `ConfigurationError` if the plugin:

- is not an async generator function;
- does not accept every contract parameter as a keyword;
- requires a parameter outside the contract, rather than giving it a default;
- has an `INJECTED` parameter that is not annotated with a class;
- does not annotate its arguments parameter with a `PluginArgs` subclass.

### Test the plugin

Plugins are plain functions, so call them directly and collect what they yield:

```python
async def test_seed_feeder_resumes_from_bookmark():
    proposals = [
        proposal
        async for proposal in seed_feeder(
            feeder_args=SeedFeederArgs(max_count=4, scouting_count=2),
            feeder_state={"next_seed": 2},
            scouting=False,
        )
    ]

    assert [p.descriptor["seed"] for p in proposals[:2]] == [2, 3]
    assert proposals[2] == SetFeederState(state={"next_seed": 4})
    assert proposals[3] == NoMoreFeederInputs()
```

Give a packer its inputs through a small async generator, and pass dependencies directly as shown in [Rules for injected parameters](#rules-for-injected-parameters). Also test that the plugin loads through its real entry point, which runs the runtime checks above:

```python
from diracx.logic.transformations import PACKER_KIND, load_plugin


def test_fixed_size_packer_is_registered():
    assert load_plugin(PACKER_KIND, "FixedSize") == (
        fixed_size_packer,
        FixedSizePackerArgs,
    )
```
