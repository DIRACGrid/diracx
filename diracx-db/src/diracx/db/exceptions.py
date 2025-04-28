from __future__ import annotations


class DBUnavailableError(Exception):
    pass


class DBInBadStateError(RuntimeError):
    pass
