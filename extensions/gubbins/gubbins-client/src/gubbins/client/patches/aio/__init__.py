"""
This shows how to define your extension aio client
"""

from __future__ import annotations

from diracx.client.patches.aio.utils import DiracClientMixin

from gubbins.client.generated.aio._client import Dirac as GubbinsGenerated


class GubbinsClient(DiracClientMixin, GubbinsGenerated): ...
