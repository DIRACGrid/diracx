# ------------------------------------
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# ------------------------------------
"""Customize generated code here.

Follow our quickstart for examples: https://aka.ms/azsdk/python/dpcodegen/python/customize
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
import importlib.util
import json
import jwt
import requests

from pathlib import Path
from typing import Any, Dict, List, Optional, cast
from urllib import parse
from azure.core.credentials import AccessToken
from azure.core.credentials import TokenCredential
from azure.core.pipeline import PipelineRequest
from azure.core.pipeline.policies import BearerTokenCredentialPolicy

from diracx.core.preferences import DiracxPreferences, get_diracx_preferences


import sys
import importlib
from importlib.abc import MetaPathFinder, Loader

__all__: List[str] = [
    "DiracClient",
]  # Add all objects you want publicly available to users at this package level


def patch_sdk():
    """Do not remove from this file.

    `patch_sdk` is a last resort escape hatch that allows you to do customizations
    you can't accomplish using the techniques described in
    https://aka.ms/azsdk/python/dpcodegen/python/customize
    """


from ..patches import DiracClient
