# Register the admin VO

The admin VO is the one that rules them all.
It is not meant to be connected to the grid services and therefore can be offline and DiracX-specific.
We currently support [Indigo IAM](https://indigo-iam.github.io/) and [dex](https://dexidp.io), but you can also use your institute SSO platform (e.g. [CERN SSO](https://sso-management.web.cern.ch)).

## Prerequisites

- You have access to your DiracX instance URL.

## Generate an OIDC client

=== "CERN SSO"

    1. Go to the [CERN Application Portal](https://application-portal.web.cern.ch/)
    2. Click "Register a new application"
    3. Fill in the application details:

    - Application Name: DiracX [Your VO/Installation]
    - Description: DiracX authentication for [Your VO]

    1. SSO Protocol: OpenID Connect
    2. Redirect URLs
        ```
        https://<youdiracx.invalid>/api/auth/device/complete
        https://<youdiracx.invalid>/api/auth/authorize/complete
        ```
    3. Base URL
        ```
        https://<youdiracx.invalid>/
        ```
    4. Client Secret Configuration: tick `My application cannot store a client secret safely`
    5. If you want to restrict to a certain egroup, in the `role` part, edit the `default role` to add your egroup

=== "Dex"

    TODO: blabla

=== "Other"

    The general guidelines for the configuration is as follow

    - **Client secret**: _none_

    - **Redirect URIs**:

        ```
        https://<your‑diracx‑url>/api/auth/authorize/complete
        https://<your‑diracx‑url>/api/auth/device/complete
        ```

    - **Grant type**: `authorization_code`

    - **Scope**: at minimum `openid`, `profile` and `email`

    ??? warning "Use Authorization Code without secrets"

        If your IDP gives you a choice, use the PKCE one, which is the only one supported by `DiracX`

## Configure DIRAC

1. In your DIRAC CS, add under `DiracX > CsSync > VOs > <VO> > IdP`:

    ??? example "Display configuration example"

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

2. Add a `diracx_admin` group to the `Registry`:

    ??? example "Dispalay configuration example"

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

3. List their subject‑IDs under `UserSubjects`:

    ??? example  "Display configuration example"

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

    ??? tip "Automate the listing of subjects if you can"

        === "CERN SSO"

            The `SubId` corresponds to the username. Members of an egroup can be listed with the following (replace `<the-egroup-you-want>`)

            ```bash
                curl --no-progress-meter --insecure "ldaps://xldap.cern.ch/OU=Users,OU=Organic Units,DC=cern,DC=ch?name?sub?(&(objectClass=user)(memberOf=CN=<the-egroup-you-want>,OU=e-groups,OU=Workgroups,DC=cern,DC=ch))"  | grep name | awk -F ":" '{print $2}' | tr -d '[:blank:]'
            ```

            ??? note "You need an ldap compatible curl build"

                See if `ldap` shows in `curl --version`

    ```
     === "Dex"

         TODO: You should do that
    ```

Make sure that the [CS sync](./convert-cs.md) is active.
