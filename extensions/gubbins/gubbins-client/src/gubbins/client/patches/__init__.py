"""
This shows how to define your extension client
"""

from diracx.client.patches.utils import DiracClientMixin

from gubbins.client.generated._client import Dirac as GubbinsGenerated


class GubbinsClient(DiracClientMixin, GubbinsGenerated): ...
