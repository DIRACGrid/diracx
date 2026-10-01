"""Exception types raised by DiracX core components."""

from __future__ import annotations

__all__ = [
    "AuthorizationError",
    "DiracError",
    "DocumentUpsertError",
    "IAMClientError",
    "IAMServerError",
    "InvalidCredentialsError",
    "InvalidQueryError",
    "NotReadyError",
    "PendingAuthorizationError",
    "PilotAlreadyAssociatedWithJobError",
    "PilotAlreadyExistsError",
    "PilotNotFoundError",
    "SandboxAlreadyAssignedError",
    "SandboxAlreadyInsertedError",
    "SandboxNotFoundError",
    "TokenNotFoundError",
]


class DiracError(RuntimeError):
    """Base exception for errors raised by DiracX.

    Attributes:
        detail: Human-readable details about the error.
    """

    def __init__(self, detail: str = "Unknown"):
        self.detail = detail
        super().__init__(detail)


class AuthorizationError(DiracError):
    """Base exception for authorization failures."""


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


class DocumentUpsertError(DiracError):
    """The backend rejected a document upsert, e.g. because it cannot be indexed."""


class TokenNotFoundError(DiracError):
    """Raised when a token cannot be found.

    Attributes:
        jti: Identifier of the missing token.
    """

    def __init__(self, jti: str, detail: str = ""):
        self.jti: str = jti
        super().__init__(f"Token {jti} not found" + (f" ({detail})" if detail else ""))


class JobNotFoundError(DiracError):
    """Raised when a job cannot be found.

    Attributes:
        job_id: Identifier of the missing job.
    """

    def __init__(self, job_id: int, detail: str = ""):
        self.job_id: int = job_id
        super().__init__(f"Job {job_id} not found" + (f" ({detail})" if detail else ""))


class SandboxNotFoundError(DiracError):
    """Raised when a sandbox cannot be found.

    Attributes:
        pfn: Physical file name of the missing sandbox.
        se_name: Storage element containing the sandbox.
    """

    def __init__(self, pfn: str, se_name: str, detail: str = ""):
        self.pfn: str = pfn
        self.se_name: str = se_name
        super().__init__(
            f"Sandbox with {pfn} and {se_name} not found"
            + (f" ({detail})" if detail else "")
        )


class ResourceNotFoundError(DiracError):
    """Raised when a named resource cannot be found.

    Attributes:
        name: Name of the missing resource.
    """

    def __init__(self, name: str, detail: str | None = None):
        self.name: str = name
        super().__init__(f"{name} not found" + (f" ({detail})" if detail else ""))


class SandboxAlreadyAssignedError(DiracError):
    """Raised when a sandbox is already assigned.

    Attributes:
        pfn: Physical file name of the sandbox.
        se_name: Storage element to which the sandbox is assigned.
    """

    def __init__(self, pfn: str, se_name: str, detail: str = ""):
        self.pfn: str = pfn
        self.se_name: str = se_name
        super().__init__(
            f"Sandbox with {pfn} and {se_name} already assigned"
            + (f" ({detail})" if detail else "")
        )


class SandboxAlreadyInsertedError(DiracError):
    """Raised when a sandbox is already inserted.

    Attributes:
        pfn: Physical file name of the sandbox.
        se_name: Storage element containing the sandbox.
    """

    def __init__(self, pfn: str, se_name: str, detail: str = ""):
        self.pfn: str = pfn
        self.se_name: str = se_name
        super().__init__(
            f"Sandbox with {pfn} and {se_name} already inserted"
            + (f" ({detail})" if detail else "")
        )


class JobError(DiracError):
    """Base exception for errors concerning a job.

    Attributes:
        job_id: Identifier of the affected job.
    """

    def __init__(self, job_id, detail: str = ""):
        self.job_id: int = job_id
        super().__init__(
            f"Error concerning job {job_id}" + (f" ({detail})" if detail else "")
        )


class NotReadyError(DiracError):
    """Tried to access a value which is asynchronously loaded but not yet available."""


class PilotNotFoundError(DiracError):
    """At least one pilot is not found."""


class PilotAlreadyExistsError(DiracError):
    """At least one pilot already exists, we avoid collisions."""


class PilotAlreadyAssociatedWithJobError(DiracError):
    """We can't associate a pilot with the same job twice."""
