The client generation is achieved via:

```
pixi run -e diracx-generate-client generate-client
```

Developers need to trigger a new client generation every time there's a change in `diracx-routers`.

DiracX also provides the reference "Gubbins" extension, and its client should be also regenerated, and the diff reviewed carefully for backwards compatibility (with the router needing to be fixed if it isn't backwards compatible):

```
pixi run -e gubbins-generate-client generate-client
```
