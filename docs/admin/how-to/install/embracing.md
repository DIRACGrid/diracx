# Embracing DiracX

Once DiracX is installed and all the VOs are enabled, you can start embracing DiracX.

!!! danger "Configure all VOs before using DiracX"

    You cannot enable any diracx service before all of your DIRAC VOs are configured in diracx. Failure to do this will result in non configured VOs being broken in DIRAC in non obvious ways which are difficult to recover from.

## Enable the S3 sandbox store

With DiracX sandboxes are stored on S3-compatible storage.
This functionality is activated for all VOs by adding the following option in the CS:

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

When this option is activated, DIRAC will forward uploads to DiracX.
Downloading sandboxes which are in DiracX is always possible regardless of this setting.
The DiracX sandbox store is configured via the environment variables in the helm chart. See the [environment variable docs](../../reference/env-variables.md) for details.

??? note "DIRAC sandbox compatibility in DiracX"

    The `DIRAC` clients still interact with the DIRAC services to proxy the sandbox files to S3. New writes only go to S3, and read can be done from both. Eventually, given the periodic cleanup of sandboxes, all the files will be on S3 only. It is not possible to download sandboxes from the DIRAC sandbox store using a DiracX client.

## Enable a DiracX service

Once all VOs have been enabled, DiracX can start replacing DIRAC services. In order to do so, "legacy adaptors" have been developed. Activating them is a manual per-service operation in the CS.

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

*Legacy Adaptors* intercept requests in the DIRAC client and redirect the traffic to DiracX services. If necessary, they also transmute the response to match the format that would have been returned by a DIRAC service.

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

Over time as more Legacy Adaptors are developed, you can enable them with the following CS option:

```
DiracX
{
    LegacyClientEnabled
    {
        <System>
        {
            <ServiceName> = True
        }
    }
}
```

The following table summarizes the DIRAC services for which a Legacy Adaptor is available (tested and certified), per DiracX release:

| DiracX release | System             | ServiceName    |
| -------------- | ------------------ | -------------- |
| v0.0.1         | WorkloadManagement | JobStateUpdate |
