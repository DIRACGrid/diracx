# Authentication with external serivices

This document explains how DIRAC and DiracX communicate with external services like storage and compute elements.

## Job submission

```mermaid
sequenceDiagram
  autonumber
  participant IAM as VO IdP
  participant CE as Computing Element
  participant DX as DiracX
  participant P as Pilot Agent

  rect Pilot submission
    DX->>IAM: Client secret flow
    IAM->>DX: WLCG token with compute scopes
    DX->>CE: Authenticate with WLCG token<br />Include unique DiracX secret with pilot payload
  end

  rect DiracX Pilot Startup Phase
    CE->>P: Start pilot job
  end
```

## Data access

### User clients

### Jobs

### File Transfer Service
