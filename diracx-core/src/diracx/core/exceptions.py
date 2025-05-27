from __future__ import annotations

from http import HTTPStatus


class DiracHttpResponseError(RuntimeError):
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


class IAMServerError(DiracError):
    """Used whenever we encounter a server problem with the IAM server."""


class IAMClientError(DiracError):
    """Used whenever we encounter a client problem with the IAM server."""


class InvalidCredentialsError(DiracError):
    """Used whenever the credentials are invalid."""


class ConfigurationError(DiracError):
    """Used whenever we encounter a problem with the configuration."""


class BadConfigurationVersionError(ConfigurationError):
    """The requested version is not known."""


class InvalidQueryError(DiracError):
    """It was not possible to build a valid database query from the given input."""


class TokenNotFoundError(DiracError):
    def __init__(self, jti: str, detail: str | None = None):
        self.jti: str = jti
        super().__init__(f"Token {jti} not found" + (f" ({detail})" if detail else ""))


class JobNotFoundError(DiracError):
    def __init__(self, job_id: int, detail: str | None = None):
        self.job_id: int = job_id
        super().__init__(f"Job {job_id} not found" + (f" ({detail})" if detail else ""))


class SandboxNotFoundError(DiracError):
    def __init__(self, pfn: str, se_name: str, detail: str | None = None):
        self.pfn: str = pfn
        self.se_name: str = se_name
        super().__init__(
            f"Sandbox with {pfn} and {se_name} not found"
            + (f" ({detail})" if detail else "")
        )


class SandboxAlreadyAssignedError(DiracError):
    def __init__(self, pfn: str, se_name: str, detail: str | None = None):
        self.pfn: str = pfn
        self.se_name: str = se_name
        super().__init__(
            f"Sandbox with {pfn} and {se_name} already assigned"
            + (f" ({detail})" if detail else "")
        )


class SandboxAlreadyInsertedError(DiracError):
    def __init__(self, pfn: str, se_name: str, detail: str | None = None):
        self.pfn: str = pfn
        self.se_name: str = se_name
        super().__init__(
            f"Sandbox with {pfn} and {se_name} already inserted"
            + (f" ({detail})" if detail else "")
        )


class JobError(DiracError):
    def __init__(self, job_id, detail: str | None = None):
        self.job_id: int = job_id
        super().__init__(
            f"Error concerning job {job_id}" + (f" ({detail})" if detail else "")
        )


class NotReadyError(DiracError):
    """Tried to access a value which is asynchronously loaded but not yet available."""
