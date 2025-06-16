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
        super().__init__(detail)


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
    def __init__(self, jti: str, detail: str = ""):
        self.jti: str = jti
        super().__init__(f"Token {jti} not found" + (f" ({detail})" if detail else ""))


class JobNotFoundError(DiracError):
    def __init__(self, job_id: int, detail: str = ""):
        self.job_id: int = job_id
        super().__init__(f"Job {job_id} not found" + (f" ({detail})" if detail else ""))


class SandboxNotFoundError(DiracError):
    def __init__(self, pfn: str, se_name: str, detail: str = ""):
        self.pfn: str = pfn
        self.se_name: str = se_name
        super().__init__(
            f"Sandbox with {pfn} and {se_name} not found"
            + (f" ({detail})" if detail else "")
        )


class SandboxAlreadyAssignedError(DiracError):
    def __init__(self, pfn: str, se_name: str, detail: str = ""):
        self.pfn: str = pfn
        self.se_name: str = se_name
        super().__init__(
            f"Sandbox with {pfn} and {se_name} already assigned"
            + (f" ({detail})" if detail else "")
        )


class SandboxAlreadyInsertedError(DiracError):
    def __init__(self, pfn: str, se_name: str, detail: str = ""):
        self.pfn: str = pfn
        self.se_name: str = se_name
        super().__init__(
            f"Sandbox with {pfn} and {se_name} already inserted"
            + (f" ({detail})" if detail else "")
        )


class JobError(DiracError):
    def __init__(self, job_id, detail: str = ""):
        self.job_id: int = job_id
        super().__init__(
            f"Error concerning job {job_id}" + (f" ({detail})" if detail else "")
        )


class NotReadyError(DiracError):
    """Tried to access a value which is asynchronously loaded but not yet available."""


class DiracFormattedError(DiracError):
    # TODO: Refactor?
    pattern = "Error %s"

    def __init__(self, data: dict[str, str], detail: str = ""):
        self.data = data

        parts = [f"({key}: {value})" for key, value in data.items()]
        message = type(self).pattern % (" ".join(parts))
        if detail:
            message += f": {detail}"

        super().__init__(message)


class PilotNotFoundError(DiracFormattedError):
    pattern = "Pilot %s not found"

    def __init__(
        self,
        data: dict[str, str],
        detail: str = "",
        non_existing_pilots: set = set(),
    ):
        super().__init__(data, detail)
        self.non_existing_pilots = non_existing_pilots


class PilotAlreadyExistsError(DiracFormattedError):
    pattern = "Pilot %s already exists"


class PilotJobsNotFoundError(DiracFormattedError):
    pattern = "Pilots or Jobs %s not found"


class PilotAlreadyAssociatedWithJobError(DiracFormattedError):
    pattern = "Pilot is already associated with a job %s "


class BadPilotCredentialsError(DiracFormattedError):
    pattern = "Bad secret/pilot_stamp %s "


class SecretNotFoundError(DiracFormattedError):
    pattern = "Secret %s not found"


class SecretHasExpiredError(DiracFormattedError):
    pattern = "Secret %s has expired"


class SecretAlreadyExistsError(DiracFormattedError):
    pattern = "Secret %s already exists"
