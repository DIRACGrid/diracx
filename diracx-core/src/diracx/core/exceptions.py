"""Custom DIRACX exception types for API and authorization errors.

This module defines the domain-specific exception hierarchy used by DIRACX
components to represent authorization failures, configuration issues, and
resource lookup errors.
"""

from __future__ import annotations

__all__ = [
    "AuthorizationError",
    "DiracError",
    "DiracHttpResponseError",
    "IAMClientError",
    "IAMServerError",
    "InvalidCredentialsError",
    "InvalidQueryError",
    "NotReadyError",
    "PendingAuthorizationError",
    "SandboxAlreadyAssignedError",
    "SandboxAlreadyInsertedError",
    "SandboxNotFoundError",
    "TokenNotFoundError",
]

from http import HTTPStatus


class DiracHttpResponseError(RuntimeError):
    """Error raised when an HTTP response from DIRACX is invalid.

    Attributes:
        status_code (int): The HTTP status code returned by the remote service.
        data: The response payload returned by the remote service.
    """

    def __init__(self, status_code: int, data):
        self.status_code = status_code
        self.data = data


class DiracError(RuntimeError):
    """Base class for DIRACX domain errors.

    Subclasses may define a specific HTTP status code and headers for API
    response generation.
    """

    http_status_code = HTTPStatus.BAD_REQUEST  # 400
    http_headers: dict[str, str] | None = None

    def __init__(self, detail: str = "Unknown"):
        """Initialize the error with a descriptive detail message.

        Args:
            detail (str): A human-readable description of the error.
        """
        self.detail = detail


class AuthorizationError(DiracError):
    """Raised when an authorization check fails."""


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
    """Raised when a token with the requested JTI cannot be found."""

    def __init__(self, jti: str, detail: str | None = None):
        """Initialize a token-not-found error.

        Args:
            jti (str): The token identifier that was not found.
            detail (str | None): Optional additional context.
        """
        self.jti: str = jti
        super().__init__(f"Token {jti} not found" + (f" ({detail})" if detail else ""))


class JobNotFoundError(DiracError):
    """Raised when a requested job cannot be located."""

    def __init__(self, job_id: int, detail: str | None = None):
        """Initialize a job-not-found error.

        Args:
            job_id (int): The missing job identifier.
            detail (str | None): Optional additional context.
        """
        self.job_id: int = job_id
        super().__init__(f"Job {job_id} not found" + (f" ({detail})" if detail else ""))


class SandboxNotFoundError(DiracError):
    """Raised when a sandbox resource cannot be found."""

    def __init__(self, pfn: str, se_name: str, detail: str | None = None):
        """Initialize a sandbox-not-found error.

        Args:
            pfn (str): The physical file name of the sandbox.
            se_name (str): The storage element name.
            detail (str | None): Optional additional context.
        """
        self.pfn: str = pfn
        self.se_name: str = se_name
        super().__init__(
            f"Sandbox with {pfn} and {se_name} not found"
            + (f" ({detail})" if detail else "")
        )


class ResourceNotFoundError(DiracError):
    def __init__(self, name: str, detail: str | None = None):
        self.name: str = name
        super().__init__(f"{name} not found" + (f" ({detail})" if detail else ""))


class SandboxAlreadyAssignedError(DiracError):
    """Raised when the sandbox is already assigned to a job."""

    def __init__(self, pfn: str, se_name: str, detail: str | None = None):
        """Initialize a sandbox-already-assigned error.

        Args:
            pfn (str): The physical file name of the sandbox.
            se_name (str): The storage element name.
            detail (str | None): Optional additional context.
        """
        self.pfn: str = pfn
        self.se_name: str = se_name
        super().__init__(
            f"Sandbox with {pfn} and {se_name} already assigned"
            + (f" ({detail})" if detail else "")
        )


class SandboxAlreadyInsertedError(DiracError):
    """Raised when a sandbox has already been inserted."""

    def __init__(self, pfn: str, se_name: str, detail: str | None = None):
        """Initialize a sandbox-already-inserted error.

        Args:
            pfn (str): The physical file name of the sandbox.
            se_name (str): The storage element name.
            detail (str | None): Optional additional context.
        """
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
