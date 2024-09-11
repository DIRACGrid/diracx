from __future__ import annotations

from ..utils import BaseSQLDB
from .schema import PilotAgentsDBBase


class PilotAgentsDB(BaseSQLDB):
    metadata = PilotAgentsDBBase.metadata
