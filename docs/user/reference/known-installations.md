# Production installations

As a user of grid infrastructure you're probably most interested in using an installation that is provided by a third party.
This page shows an incomplete list of DiracX installations in alphabetical order.

!!! tip

    If your installation is missing, please use the edit button in the top right to add it!

## LHCb

[LHCb](https://lhcb-outreach.web.cern.ch/){target="\_blank"} is one of the four large experiments of the Large Hadron Collider at CERN and DiracX is used for all of their distributed computing activities.

=== "Client setup"

    From within the LHCb environment ("LbEnv") you can open a shell with LHCbDiracX by running:

    ```bash title="$ lb-dirac"
    $ dirac login lhcb
    Logging in with scopes: ['vo:lhcb']
    Now go to: https://lhcbdiracx.app.cern.ch/api/auth/device?user_code=ABCDEFGH
    ......
    Login successful!
    $ dirac jobs search
    No jobs found
    ```

=== "LHCbDiracX Web"

    [Click to open in a new tab :fontawesome-solid-globe:](https://lhcbdiracx.app.cern.ch/){ .md-button .md-button--primary target="\_blank" }

=== "Manual client configuration"

    LHCb users are encouraged to use the central CVMFS installation of LHCbDiracX instead of having a local installation.
    For users who still want to do things manually, the environment variable to set is:

    ```bash
    export DIRACX_URL=https://lhcbdiracx.app.cern.ch/
    ```

# Developers' installation

We provide a developers' installation at https://diracx-cert.app.cern.ch/. The installation is mostly used for testing and certifying candidate releases.

The installation uses the [`dteam` Virtual Organization](https://dteam-auth.cern.ch/){target="\_blank"} and as such is used to access Grid resources. The installation is using the latest pre-release of DiracX and as such can not be considered a stable and fully reliable installation. If nevertheless:

- you are part of `dteam` (becoming member is an easy process)
- you would like to access an existing DiracX installation, and see it "in action"
    [get in contact](https://web.diracgrid.org/contacts.html) explaining your use-case.
