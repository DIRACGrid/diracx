# Use DiracX functionalities

DiracX functionalities can be enabled at your pace.

## Enable the VO

With "enabling a VO" we mean that a Virtual Organization enjoys DiracX functionalities.
By default, every VO managed within a DIRAC (v9+) installation is enabled.
If, for whatever reason (e.g. the IdP for a certain VO is not setup),
such VO can be disabled by adding it in the following list (which is empty by default).

```
DiracX
{
    DisabledVOs =
}
```

-> gives you token

ToDO

## Enable the S3 sandbox store

With DiracX the Sandboxes can be stored in an S3-compatible sandbox store.
This functionality will be activated for all the enabled VOs by adding the following option in the CS:

```
Systems
{
    Services
    {
        SandboxStore
        {
            UseDiracXBackend = True
        }
    }
}
```

When this option is activated, DIRAC will forward to DiracX the upload/download of sandboxes.
DiracX will find the correct settings in the `DIRACX_SANDBOX_STORE_*` [environment variables](../../reference/env_variables.md).

!!! warning

    The `DIRAC` clients still interact with the DIRAC services to proxy the sandbox files to S3. New writes only go to S3, and read can be done from both. Eventually, given the periodic cleanup of sandboxes, all the files will be on S3 only.

## Enable a DiracX service

Once all VOs have been enabled, DiracX can start replacing DIRAC services. In order to do so, "legacy adaptors" have been developed. Activating them is a manual operation in the CS.

The following sequence of images explain the process:

DBs are shared, so DIRAC DIPS services and DiracX HTTP services are looking into the same database

```mermaid
flowchart TB
    DB[(DB)]:::db
    DIPS(["DIPS service"]):::service
    DIRACClient(["DIRAC client class"]):::client
    DIRACXService(["DiracX service"]):::service
    DIRACXClient(["DiracX client class"]):::client

    DB --- DIPS
    DB --- DIRACXService
    DIPS --- DIRACClient
    DIRACXService --- DIRACXClient

    classDef db fill:#ffe6e6,stroke:#333,stroke-width:1px;
    classDef service fill:#e6ffe6,stroke:#333,stroke-width:1px;
    classDef client fill:#ffe6e6,stroke:#333,stroke-width:1px;
    classDef adaptor fill:#e6f0ff,stroke:#333,stroke-width:1px;

```

*Legacy Adaptors* are coded to move the traffic from DIRAC clients to DiracX services.

```mermaid
flowchart TB
    DB[(DB)]:::db
    DIPS(["DIPS service"]):::service
    DIRACClient(["DIRAC client class"]):::client
    DIRACXService(["DiracX service"]):::service
    DIRACXClient(["DiracX client class"]):::client
    LegacyAdaptor{"Legacy adaptor"}:::adaptor

    DB --- DIPS
    DB --- DIRACXService
    LegacyAdaptor --- DIRACClient
    DIRACXService --- DIRACXClient
    DIRACXService --- LegacyAdaptor

    classDef db fill:#ffe6e6,stroke:#333,stroke-width:1px;
    classDef service fill:#e6ffe6,stroke:#333,stroke-width:1px;
    classDef client fill:#ffe6e6,stroke:#333,stroke-width:1px;
    classDef adaptor fill:#e6f0ff,stroke:#333,stroke-width:1px;

```

At this point, DIRAC services can be removed:

```mermaid
flowchart TB
    DB[(DB)]:::db
    DIRACClient(["DIRAC client class"]):::client
    DIRACXService(["DiracX service"]):::service
    DIRACXClient(["DiracX client class"]):::client
    LegacyAdaptor{"Legacy adaptor"}:::adaptor

    DB --- DIRACXService
    LegacyAdaptor --- DIRACClient
    DIRACXService --- DIRACXClient
    DIRACXService --- LegacyAdaptor

    classDef db fill:#ffe6e6,stroke:#333,stroke-width:1px;
    classDef service fill:#e6ffe6,stroke:#333,stroke-width:1px;
    classDef client fill:#ffe6e6,stroke:#333,stroke-width:1px;
    classDef adaptor fill:#e6f0ff,stroke:#333,stroke-width:1px;

```

Legacy adaptors will eventually be created for each of the DIRAC services.
In order to activate one, it is enough to add the following CS option:

```
DiracX
{
    LegacyClientEnabled
    {
        DIRAC_System
        {
            DIRAC_ServiceName = True
        }
    }
}
```

The following table summarizes the DIRAC services for which a Legacy Adaptor is available (tested and certified), per DiracX release:

| DiracX release | DIRAC_System       | DIRAC_ServiceName |
| -------------- | ------------------ | ----------------- |
| v0.1.0         | WorkloadManagement | JobStateUpdate    |
