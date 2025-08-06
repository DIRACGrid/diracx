## Adding a router

To add a router there are two steps:

1. Create a module in `diracx.routers` for the given service.
2. Add an entry to the `diracx.services` entrypoint.
3. Do not forget the Access Policy (see chapter lower down)

We'll now make a `/parking/` router which contains information store in the `DummyDB`.

### Creating the router submodule

TODO: This isn't yet documented here however see `diracx.routers.auth.well_known` for an example.

### Defining the new router in the `diracx.services` entrypoint

Modify the package's `setup.cfg`

```ini
[options.entry_points]
diracx.services =
	parking = diracx.routers.parking:router
```

This will prefix the routes with `/parking/` and mark them with the `"parking"` tag in the OpenAPI spec.

!!! warning

    Any modification in the `pyproject.toml` requires to re-install install `diracx`, even if it is a developer installation (`pip install -e`)
