from __future__ import annotations

from importlib.metadata import entry_points

import dotenv
from fastapi import Depends, Request
from fastapi.responses import JSONResponse, Response
from pydantic import Field

from diracx.core.exceptions import DiracError, DiracHttpResponse
from diracx.core.utils import dotenv_files_from_environment
from diracx.routers.auth import AuthSettings
from diracx.routers.configuration import ConfigSettings
from diracx.routers.job_manager import JobsSettings

from .auth import router as auth_router
from .auth import verify_dirac_token
from .fastapi_classes import DiracFastAPI, ServiceSettingsBase
from .well_known import WellKnownSettings
from .well_known import router as well_known_router

# Rules:
# All routes must have tags (needed for auto gen of client)
# Form headers must have a description (autogen)
# methods name should follow the generate_unique_id_function pattern


def create_app_inner(
    *all_service_settings: ServiceSettingsBase,
) -> DiracFastAPI:
    app = DiracFastAPI()

    class_to_settings = {}
    for service_settings in all_service_settings:
        if type(service_settings) in class_to_settings:
            raise NotImplementedError(f"{type(service_settings)} has been reused")
        class_to_settings[type(service_settings)] = service_settings

    for entry_point in entry_points().select(group="diracx.services"):
        router = entry_point.load()
        service_settings = class_to_settings[router.settings_class]
        if router is auth_router or router is well_known_router:
            app.include_router(router, settings=service_settings)
        else:
            app.include_router(
                router,
                settings=service_settings,
                dependencies=[Depends(verify_dirac_token)],
            )

    # Add exception handlers
    app.add_exception_handler(DiracError, dirac_error_handler)
    app.add_exception_handler(DiracHttpResponse, http_response_handler)

    return app


class DiracxSettings(ServiceSettingsBase, env_prefix="DIRACX_SERVICE_ENABLED_"):
    auth: AuthSettings | None = Field(default_factory=AuthSettings)
    config: ConfigSettings | None = Field(default_factory=ConfigSettings)
    jobs: JobsSettings | None = Field(default_factory=JobsSettings)
    well_known: WellKnownSettings | None = Field(default_factory=WellKnownSettings)


def create_app() -> DiracFastAPI:
    for env_file in dotenv_files_from_environment("DIRACX_SERVICE_DOTENV"):
        if not dotenv.load_dotenv(env_file):
            raise NotImplementedError(f"Could not load dotenv file {env_file}")
    return create_app_inner(*dict(DiracxSettings()._iter()).values())


def dirac_error_handler(request: Request, exc: DiracError) -> Response:
    return JSONResponse(
        status_code=exc.http_status_code, content={"detail": exc.detail}
    )


def http_response_handler(request: Request, exc: DiracHttpResponse) -> Response:
    return JSONResponse(status_code=exc.status_code, content=exc.data)
