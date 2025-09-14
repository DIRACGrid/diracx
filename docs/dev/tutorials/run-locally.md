# Run a full DiracX demo

For the majority of developments it is not necessary to run a full DiracX instance locally.
It's also strongly discouraged as it requires:

- Moderately powerful hardware and good network access.
- A good understanding of how to administer a DiracX installation.
- Much more time than simply running the tests (several minutes vs several seconds).

That said, DiracX is designed to make it as easy as possible to run a full instance locally if you desire and it can be an invaluable tool and this set up is known as **"the demo"**.

## Prerequisites

Before starting the demo requires:

<div class="annotate" markdown>
* **An Unix-like operating system:** The DiracX tests are only officially supported on Unix-like operating systems like macOS and Linux. If you're running Windows we would suggest using the [Windows Subsystem for Linux](https://learn.microsoft.com/en-us/windows/wsl/install).
* **Docker:** On Linux we recommend Docker Engine [installation instructions](https://docs.docker.com/engine/install/). On macOS we recommend [Docker Desktop](https://docs.docker.com/desktop/setup/install/mac-install/). (1)
</div>

1. Commercial use of Docker Desktop is subject to restrictions, if this is a problem for you we would be happy to accept contributions to adapt the demo to support alternate container runtimes. Please [open an issue](https://github.com/DIRACGrid/diracx-charts/issues/new).

!!! tip

    If you're not interested in developing DiracX, check out the [administrator guide](../../admin/tutorials/run_locally.md).

## Getting the code

First clone your fork of the `diracx` and `diracx-charts` repos:

=== "SSH"

    First make a fork on the GitHub web interface and then run:

    ```bash
    git clone git@github.com:<GitHub username>/diracx.git
    cd diracx
    git remote add upstream git@github.com:DIRACGrid/diracx.git
    cd ..
    git clone git@github.com:<GitHub username>/diracx-charts.git
    cd diracx-charts
    git remote add upstream git@github.com:DIRACGrid/diracx-charts.git
    ```

=== "GitHub CLI"

    ```bash
    gh repo fork DIRACGrid/diracx # (1)!
    gh repo fork DIRACGrid/diracx-charts
    ```

    1. The GitHub CLI can be installed as a [`pixi global` tool](https://pixi.sh/dev/global_tools/introduction/) using:

        ```bash
        pixi global install gh
        ```

=== "HTTPS"

    First make a fork on the GitHub web interface and then run:

    ```bash
    git clone https://github.com/<GitHub username>/diracx.git
    cd diracx
    git remote add upstream https://github.com/DIRACGrid/diracx.git
    cd ..
    git clone https://github.com/<GitHub username>/diracx-charts.git
    cd diracx-charts
    git remote add upstream https://github.com/DIRACGrid/diracx-charts.git
    ```

## Starting the demo

--8<-- "docs/dev/how-to/run-demo.md"
