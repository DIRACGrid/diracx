from __future__ import annotations

import os

import pytest

# This fixture makes sure the extension variable is set correctly
# during the tests.
# You really should define one like that, it will save you some headache


@pytest.fixture(scope="session", autouse=True)
def check_extension_env():
    if os.environ.get("DIRACX_EXTENSIONS") != "gubbins,diracx":
        pytest.fail(
            "You must set the DIRACX_EXTENSIONS environment variable to 'gubbins,diracx'"
        )
