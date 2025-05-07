# Register the admin VO

The admin VO is the one that rules them all.
It is not meant to be connected to the grid services and therefore can be offline and DiracX-specific.
We currently support [Indigo IAM](https://indigo-iam.github.io/) and [dex](https://dexidp.io), but you can also use your institute SSO platform (e.g. [CERN SSO](https://sso-management.web.cern.ch)).

## Prerequisites

- You have access to your DiracX instance URL.

## 1. Generate an OIDC client

1. Log in to your VO’s IdP instance (e.g. Indigo IAM).
2. Create a new OIDC client with:
   - **Client secret**: _none_
   - **Redirect URIs**:
     ```
     https://<your‑diracx‑url>/api/auth/authorize/complete
     ```
   - **Grant type**: `authorization_code`
   - **Scope**: at minimum `openid`, `profile` and `email`

## 2. Configure DiracX

1. In your DIRAC CS, add under `DiracX > CsSync > VOs > <VO> > IdP`:

   ```yaml
   DiracX
   {
     CsSync
     {
       VOs
       {
         diracx_admin
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

2. Add an admin group to the `Registry`:

   ```yaml
   Registry
   {
     Groups
     {
       diracx_admin
       {
         Users = <username from dirac>
         VO = diracx_admin
         Properties = Operator
         Properties += FullDelegation
         Properties += ProxyManagement
         Properties += ServiceAdministrator
         Properties += JobAdministrator
         Properties += CSAdministrator
         Properties += AlarmsManagement
         Properties += FileCatalogManagement
         Properties += SiteManager
       }
     }
   }
   ```

3. To add specific users, list their subject‑IDs under `UserSubjects`:

   ```yaml
   DiracX
   {
     CsSync
     {
       VOs
       {
         diracx_admin
         {
           UserSubjects
           {
             <username from dirac> = <user id from the IdP instance>
             ...
           }
           DefaultGroup = diracx_admin
         }
       }
     }
   }
   ```

After saving, you should sync the configuration with DiracX.
See [Convert CS](./convert-cs.md) for next steps.
