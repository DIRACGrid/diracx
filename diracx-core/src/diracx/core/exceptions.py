from http import HTTPStatus


class DiracHttpResponse(RuntimeError):
    def __init__(self, status_code: int, data):
        self.status_code = status_code
        self.data = data


class DiracError(RuntimeError):
    http_status_code = HTTPStatus.BAD_REQUEST  # 400
    http_headers: dict[str, str] | None = None

    def __init__(self, detail: str = "Unknown"):
        self.detail = detail


class AuthorizationError(DiracError): ...


class PendingAuthorizationError(AuthorizationError):
    """Used to signal the device flow the authentication is still ongoing."""


class ExpiredFlowError(AuthorizationError):
    """Used only for the Device Flow when the polling is expired."""


class ConfigurationError(DiracError):
    """Used whenever we encounter a problem with the configuration."""


class BadConfigurationVersion(ConfigurationError):
    """The requested version is not known."""


class InvalidQueryError(DiracError):
    """It was not possible to build a valid database query from the given input."""


class JobNotFound(Exception):
    def __init__(self, job_id: int):
        self.job_id: int = job_id
        super().__init__(f"Job {job_id} not found")
