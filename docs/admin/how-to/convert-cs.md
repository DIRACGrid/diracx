# Converting the DIRAC CS for DiracX

Currently the source of truth for the configuration in DiracX comes from your legacy DIRAC installation and this information needs to be synchronized.

## Converting the DIRAC CS manually

1. Clone the Git repository containing the DiracX CS (can be empty)

    ```bash
    git clone git@your-host.invalid/dirac/diracx-config.git diracx-config
    ```

2. Use a DIRAC client installation to read the DIRAC CS from the machine hosting the master CS:

    ```bash
    DIRAC_COMPAT_ENABLE_CS_CONVERSION=true dirac internal legacy cs-sync /opt/dirac/etc/YOUR_SETUP.cfg diracx-config/default.yml
    ```

    ??? example "Expand to see how to do this from a different machine using a proxy"

        The following Python snippet can be run on any client with a proxy:

        ```python
        #!/usr/bin/env python
        import subprocess
        import os
        import tempfile
        import zlib
        from pathlib import Path

        import DIRAC

        DIRAC.initialize()
        from DIRAC import gConfig
        from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
        from DIRAC.ConfigurationSystem.Client.ConfigurationClient import ConfigurationClient

        client = ConfigurationClient(
            url=gConfig.getValue("/DIRAC/Configuration/MasterServer", "")
        )
        data = returnValueOrRaise(client.getCompressedData())
        data = zlib.decompress(data)
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(data)
            tmp.flush()
            cmd = ["dirac", "internal", "legacy", "cs-sync", tmp.name, "default.yml"]
            subprocess.run(
                cmd, env=os.environ | {"DIRAC_COMPAT_ENABLE_CS_CONVERSION": "yes"}, check=True
            )

        print("Synced CS to default.yml, now you can review the changes and commit/push them")
        ```

3. Commit the changes and push

    ```bash
    cd diracx-config
    git status
    # If nothing has changed you're done!
    git add default.yml
    git commit -m "Convert from DIRAC CS"
    git push
    ```

## Converting the DIRAC CS automatically

To keep the DiracX CS in sync we recommend setting up a cron job on the machine running the master CS to run regularly.
For example to run every 5 minutes use [`crontab -e`](https://linux.die.net/man/5/crontab) to create:

```cron
*/5 * * * * /path/to/sync/script.sh
```

??? note "Example sync script"

    ```bash
    #!/bin/bash
    set -euo pipefail

    INSTALLATION_BASHRC=/cvmfs/..../bashrc
    REPO_URL=git@your-host.invalid/dirac/diracx-config.git

    date

    usage() {
        echo "Usage: $0 --master-cs-file <path-to-file>"
        exit 1
    }

    # Parse the named parameters
    while [[ "$#" -gt 0 ]]; do
        case $1 in
            --master-cs-file) MASTER_CS_PATH="$2"; shift ;;
            *) usage ;;
        esac
        shift
    done

    # Check if both parameters are provided
    if [ -z "$MASTER_CS_PATH" ]; then
        usage
    fi

    # Disable the -u option as the bashrc may have unbound variables
    set +u
    source "$INSTALLATION_BASHRC"
    set -u

    export _TYPER_STANDARD_TRACEBACK=1

    MASTER_CS_FILE=$(basename "${MASTER_CS_PATH}")

    TEMP_DIR=$(mktemp -d --suffix=_cs_sync)
    cd "$TEMP_DIR"

    # Clone the repository
    git clone "$REPO_URL" repo

    # Navigate to the cloned repository
    cd repo

    git config user.name "Master CS Sync"
    git config user.email "master-cs-sync@invalid"


    # Copy the config, modify it and update the yaml config
    cp "${MASTER_CS_PATH}" "$TEMP_DIR"/
    DIRAC_COMPAT_ENABLE_CS_CONVERSION=True dirac internal legacy cs-sync "$TEMP_DIR/${MASTER_CS_FILE}" default.yml

    git diff

    git add default.yml
    if git commit -m "CS cron $(hostname)"; then
        git push origin master
    fi

    cd
    rm -rf "$TEMP_DIR"
    ```
