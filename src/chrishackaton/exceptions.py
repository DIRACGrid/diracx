class AuthorizationError(RuntimeError):
    ...


class PendingAuthorizationError(AuthorizationError):
    """Used to signal the device flow the authentication is still ongoing"""

    ...
