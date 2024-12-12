from __future__ import annotations

from contextlib import nullcontext
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from diracx.core.properties import (
    GENERIC_PILOT,
    NORMAL_USER,
    OPERATOR,
    PILOT,
    SERVICE_ADMINISTRATOR,
)
from diracx.routers.pilot_logging.access_policies import (
    ActionType,
    PilotLogsAccessPolicy,
)


@pytest.mark.parametrize(
    "user, action, expectation",
    [
        (PILOT, ActionType.CREATE, nullcontext()),
        (PILOT, ActionType.QUERY, pytest.raises(HTTPException, match="403")),
        (PILOT, ActionType.DELETE, pytest.raises(HTTPException, match="403")),
        (GENERIC_PILOT, ActionType.CREATE, nullcontext()),
        (GENERIC_PILOT, ActionType.QUERY, pytest.raises(HTTPException, match="403")),
        (GENERIC_PILOT, ActionType.DELETE, pytest.raises(HTTPException, match="403")),
        (SERVICE_ADMINISTRATOR, ActionType.CREATE, nullcontext()),
        (SERVICE_ADMINISTRATOR, ActionType.QUERY, nullcontext()),
        (SERVICE_ADMINISTRATOR, ActionType.DELETE, nullcontext()),
        (OPERATOR, ActionType.CREATE, nullcontext()),
        (OPERATOR, ActionType.QUERY, nullcontext()),
        (OPERATOR, ActionType.DELETE, nullcontext()),
        (NORMAL_USER, ActionType.CREATE, pytest.raises(HTTPException, match="403")),
        (NORMAL_USER, ActionType.QUERY, nullcontext()),
        (NORMAL_USER, ActionType.DELETE, pytest.raises(HTTPException, match="403")),
        (
            "malicious_user",
            ActionType.CREATE,
            pytest.raises(HTTPException, match="403"),
        ),
        ("malicious_user", ActionType.QUERY, pytest.raises(HTTPException, match="403")),
        (
            "malicious_user",
            ActionType.DELETE,
            pytest.raises(HTTPException, match="403"),
        ),
        ("any_user", None, pytest.raises(HTTPException, match="400")),
    ],
)
async def test_access_policies(user, action, expectation):
    user_info = MagicMock()
    user_info.properties = [user]
    with expectation:
        ret = await PilotLogsAccessPolicy.policy(
            "PilotLogsAccessPolicy", user_info, action=action
        )
        assert user in ret.properties
