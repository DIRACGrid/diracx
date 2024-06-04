from __future__ import annotations

import pytest


# This fixture sets the environment variable
# during the tests.
# You MUST define one like that
@pytest.fixture(scope="session", autouse=True)
def set_extension_env():
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("DIRACX_EXTENSIONS", "gubbins,diracx")
        yield
