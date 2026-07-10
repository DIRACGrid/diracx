from __future__ import annotations

__all__ = ["DiracHttpResponseError"]


class DiracHttpResponseError(RuntimeError):
    def __init__(self, status_code: int, data):
        self.status_code = status_code
        self.data = data
