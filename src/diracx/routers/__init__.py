from __future__ import annotations

import os
from importlib.metadata import entry_points

import dotenv
from fastapi import Depends, Request
from fastapi.responses import JSONResponse, Response
from pydantic import parse_raw_as

from diracx.core.exceptions import DiracError, DiracHttpResponse
from diracx.core.utils import dotenv_files_from_environment

from .auth import verify_dirac_token
from .fastapi_classes import DiracFastAPI, DiracxRouter, ServiceSettingsBase

# Rules:
# All routes must have tags (needed for auto gen of client)
# Form headers must have a description (autogen)
# methods name should follow the generate_unique_id_function pattern


def create_app_inner(
    *all_service_settings: ServiceSettingsBase,
) -> DiracFastAPI:
    app = DiracFastAPI()

    class_to_settings: dict[type[ServiceSettingsBase], ServiceSettingsBase] = {}
    for service_settings in all_service_settings:
        if type(service_settings) in class_to_settings:
            raise NotImplementedError(f"{type(service_settings)} has been reused")
        class_to_settings[type(service_settings)] = service_settings

    for entry_point in entry_points().select(group="diracx.services"):
        router: DiracxRouter = entry_point.load()
        if router.diracx_settings_class not in class_to_settings:
            continue
        app.include_router(
            router,
            settings=class_to_settings[router.diracx_settings_class],
            prefix=f"/{entry_point.name}",
            tags=[entry_point.name],
            dependencies=[Depends(verify_dirac_token)]
            if router.diracx_require_auth
            else [],
        )

    # Add exception handlers
    app.add_exception_handler(DiracError, dirac_error_handler)
    app.add_exception_handler(DiracHttpResponse, http_response_handler)

    return app


def create_app() -> DiracFastAPI:
    for env_file in dotenv_files_from_environment("DIRACX_SERVICE_DOTENV"):
        if not dotenv.load_dotenv(env_file):
            raise NotImplementedError(f"Could not load dotenv file {env_file}")

    all_service_settings = []
    for entry_point in entry_points().select(group="diracx.services"):
        router: DiracxRouter = entry_point.load()
        env_prefix = router.diracx_settings_class.__config__.env_prefix
        enabled = parse_raw_as(bool, os.environ.get(f"{env_prefix}ENABLED", "true"))
        if enabled:
            all_service_settings.append(router.diracx_settings_class())

    return create_app_inner(*all_service_settings)


def dirac_error_handler(request: Request, exc: DiracError) -> Response:
    return JSONResponse(
        status_code=exc.http_status_code, content={"detail": exc.detail}
    )


def http_response_handler(request: Request, exc: DiracHttpResponse) -> Response:
    return JSONResponse(status_code=exc.status_code, content=exc.data)
