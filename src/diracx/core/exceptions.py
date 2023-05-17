from fastapi import status


class DIRACError(RuntimeError):
    http_status_code = status.HTTP_400_BAD_REQUEST

    def __init__(self, detail="Unknown"):
        self.detail = detail


class AuthorizationError(DIRACError):
    ...


class PendingAuthorizationError(AuthorizationError):
    """Used to signal the device flow the authentication is still ongoing"""


class ExpiredFlowError(AuthorizationError):
    """Used only for the Device Flow when the polling is expired"""


class ConfigurationError(DIRACError):
    """Used whenever we encounter a problem with the configuration"""


class BadConfigurationVersion(ConfigurationError):
    """The requested version is not known"""


class InvalidQueryError(DIRACError):
    """It was not possible to build a valid database query from the given input"""
