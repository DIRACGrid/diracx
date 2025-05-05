# Configuration

This page describes the way by which configuration is handled within DiracX.
Configuration refers to the central store of configuration data which is made available to both servers and clients.
This is in contrast to "Settings" which are only available on the server side.
Confidential information (such as passwords) is only handled in Settings, see the DiracX helm chart for details.

The DiracX configuration is stored as a single YAML file.
We recommend that this is stored within a Git repository, and DiracX provides two git-based backends can be used by servers:

* `git+file`: Refers to a local git repository. This must be stored on a shared volume which is made available to all DiracX servers.
* `git+https`: Refers to a remote git repository that can be stored on any standard git host.

## Structure of the CS

The canonical way of accessing the DiracX configuration from within code is via the corresponding pydantic model.
This provides strong typing of all values in the CS and enables the schema of the `default.yml` file to be validated.
The pydantic model is defined in `diracx.core.config`.

## Client access

The DiracX configuration is made available to clients via the `/api/config/` route.
To allow for updates to be quickly and efficiently propagated to clients, DiracX respects the [`If-None-Match`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-None-Match) and [`If-Modified-Since`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-Modified-Since) headers.
These headers can be used to efficiently check for updates without needing to download the entire contents of the configuration.

## Modifying configuration

The diracx configuration differs in structure and contents from the legacy DIRAC Configuration Service. Contrary to DIRAC's legacy configuration, which is stored in a custom [cfg](https://github.com/DIRACGrid/diraccfg) format, the diracx configuration is stored in a standard yaml format.
Apart from these differences, DIRAC and DiracX need to share the same information, so a way to go from DIRAC configuration to DiracX one needs to be there, and the configurations should be in sync.

The canonical source of configuration is from the legacy DIRAC Configuration Service.
We foresee this will continue to be the case until the migration from DIRAC -> DiracX is complete.
During this time, the DiracX configuration is not intended to be edited directly.
The legacy DIRAC CFG file can be converted into the new YAML format with:

```bash
DIRAC_COMPAT_ENABLE_CS_CONVERSION=true dirac internal legacy cs-sync dirac-cs.cfg diracx-config/default.yml
```

The following script serves as wrapper for running the above command, and can be run on any diracx client (so, on any DIRAC v9+ client or server) with a proxy:

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

client = ConfigurationClient(url=gConfig.getValue("/DIRAC/Configuration/MasterServer", ""))
data = returnValueOrRaise(client.getCompressedData())
data = zlib.decompress(data)
with tempfile.NamedTemporaryFile() as tmp:
    tmp.write(data)
    tmp.flush()
    cmd = ["dirac", "internal", "legacy", "cs-sync", tmp.name, "default.yml"]
    subprocess.run(cmd, env=os.environ | {"DIRAC_COMPAT_ENABLE_CS_CONVERSION": "yes"}, check=True)

print("Synced CS to default.yml, now you can review the changes and commit/push them")
```

TODO: Document how we will actually do the sync for production deployments...
