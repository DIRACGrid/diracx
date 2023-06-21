from __future__ import annotations

import dotenv
from fastapi import Depends, Request
from fastapi.responses import JSONResponse, Response
from pydantic import Field

from diracx.core.exceptions import DiracError, DiracHttpResponse
from diracx.core.properties import SecurityProperty
from diracx.core.utils import dotenv_files_from_environment
from diracx.routers.auth import AuthSettings
from diracx.routers.configuration import ConfigSettings
from diracx.routers.job_manager import JobsSettings

from .auth import has_properties, verify_dirac_token
from .auth import router as auth_router
from .configuration import router as configuration_router
from .fastapi_classes import DiracFastAPI, ServiceSettingsBase
from .job_manager import router as job_manager_router
from .well_known import router as well_known_router

# Rules:
# All routes must have tags (needed for auto gen of client)
# Form headers must have a description (autogen)
# methods name should follow the generate_unique_id_function pattern


def create_app_inner(
    jobs: JobsSettings | None, config: ConfigSettings | None, auth: AuthSettings | None
) -> DiracFastAPI:
    app = DiracFastAPI()

    # Add routers
    if jobs:
        app.include_router(
            job_manager_router,
            prefix="/jobs",
            settings=jobs,
            dependencies=[Depends(verify_dirac_token)],
        )

    if config:
        app.include_router(
            configuration_router,
            prefix="/config",
            dependencies=[
                Depends(verify_dirac_token),
                has_properties(SecurityProperty.NORMAL_USER),
            ],
            settings=config,
        )

    if auth:
        app.include_router(auth_router, prefix="/auth", settings=auth)

    app.include_router(well_known_router)

    # Add exception handlers
    app.add_exception_handler(DiracError, dirac_error_handler)
    app.add_exception_handler(DiracHttpResponse, http_response_handler)

    return app


class DiracxSettings(ServiceSettingsBase, env_prefix="DIRACX_SERVICE_ENABLED_"):
    auth: AuthSettings | None = Field(default_factory=AuthSettings)
    config: ConfigSettings | None = Field(default_factory=ConfigSettings)
    jobs: JobsSettings | None = Field(default_factory=JobsSettings)


def create_app() -> DiracFastAPI:
    for env_file in dotenv_files_from_environment("DIRACX_SERVICE_DOTENV"):
        if not dotenv.load_dotenv(env_file):
            raise NotImplementedError(f"Could not load dotenv file {env_file}")
    return create_app_inner(**dict(DiracxSettings()._iter()))


def dirac_error_handler(request: Request, exc: DiracError) -> Response:
    return JSONResponse(
        status_code=exc.http_status_code, content={"detail": exc.detail}
    )


def http_response_handler(request: Request, exc: DiracHttpResponse) -> Response:
    return JSONResponse(status_code=exc.status_code, content=exc.data)
