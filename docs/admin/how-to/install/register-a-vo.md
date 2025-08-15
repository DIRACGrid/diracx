# Register a VO

## Prerequisites

- You have access to your DiracX instance URL.
- It is better to have admin rights on the VO’s IdP instance (e.g. [Indigo IAM](https://indigo-iam.github.io/)).

## Manage Users

### 1. Generate an OIDC client

1. Log in to your VO’s IdP instance.
2. Create a new OIDC client with:
    - **Client secret**: _none_
    - **Redirect URIs**:
        ```
        https://<your‑diracx‑url>/api/auth/authorize/complete
        https://<your‑diracx‑url>/api/auth/device/complete
        ```
    - **Grant type**: `authorization_code`
    - **Scope**: at minimum `openid`, `profile` and `email`

### 1. Configure DiracX

1. In your DIRAC CS, add under `DiracX > CsSync > VOs > <VO> > IdP`:

    ??? example "Configuration example"

        ```yaml
        DiracX
        {
          CsSync
          {
            VOs
            {
              <VO>
              {
                IdP
                {
                  ClientID = "<OIDC‑client‑ID>"
                  URL = "https://<your‑idp‑instance>/"
                }
              }
            }
          }
        }
        ```

2. To add specific users, list their subject‑IDs under `UserSubjects`:

    ??? example "Configuration example"

        ```yaml
        DiracX
        {
          CsSync
          {
            VOs
            {
              <VO>
              {
                UserSubjects
                {
                  <username from dirac> = <user id from the IdP instance>
                  ...
                }
              }
            }
          }
        }
        ```

    !!! note

        User IDs are associated to the usernames that are defined in the `Registry > Users` section. This allows DiracX to retrieve the groups they belong to and their properties.

    !!! success "Automatic population of the UserSubjects"

        The `DIRAC VOMS2CSAgent` can populate this list for you from an `IAM` server. For that you need to enable the `UseIAM` flag to True.

After saving, you should sync the configuration with DiracX. Dirac Groups and properties should then be associated to users defined in the `DiracX` section.
See the previous step [Convert CS](./convert-cs.md) for a refresher.

## Interact with Computing Elements

Client configuration is still managed through the DIRAC configuration so far. See [Submitting pilots to CEs using tokens](https://dirac.readthedocs.io/en/latest/AdministratorGuide/HowTo/pilotsWithTokens.html) for further details.

## Interact with Storage Elements

This isn't possible yet as the interaction hasn't yet been finalized by WLCG. See [the roadmap](../../../roadmap.md) for details.
