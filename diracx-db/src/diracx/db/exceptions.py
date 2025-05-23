from __future__ import annotations


class DBUnavailableError(Exception):
    pass


class DBInBadStateError(RuntimeError):
    def __init__(self, detail: str = "Unknown"):
        self.detail = detail
