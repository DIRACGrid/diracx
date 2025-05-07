The legacy DIRAC CFG file can be converted into the new YAML format with:

```bash
DIRAC_COMPAT_ENABLE_CS_CONVERSION=true dirac internal legacy cs-sync dirac-cs.cfg diracx-config/default.yml
```

The following can be run on any client with a proxy

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

TODO: Document how we will actually do the sync for production deployments...
