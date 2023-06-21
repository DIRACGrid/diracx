from __future__ import annotations

import asyncio
import contextlib

import dotenv
from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse, Response

from diracx.core.exceptions import DiracError, DiracHttpResponse
from diracx.core.secrets import AuthSecrets, ConfigSecrets, JobsSecrets, get_secrets
from diracx.core.utils import dotenv_files_from_environment

from .auth import DIRAC_CLIENT_ID, verify_dirac_token
from .auth import router as auth_router
from .configuration import router as configuration_router
from .job_manager import router as job_manager_router
from .well_known import router as well_known_router

# Rules:
# All routes must have tags (needed for auto gen of client)
# Form headers must have a description (autogen)
# methods name should follow the generate_unique_id_function pattern


class DiracFastAPI(FastAPI):
    def __init__(self):
        @contextlib.asynccontextmanager
        async def lifespan(app: DiracFastAPI):
            async with contextlib.AsyncExitStack() as stack:
                await asyncio.gather(
                    *(stack.enter_async_context(f()) for f in app.lifetime_functions)
                )
                yield

        self.lifetime_functions = []
        super().__init__(
            swagger_ui_init_oauth={
                "clientId": DIRAC_CLIENT_ID,
                "scopes": "property:NormalUser",
                "usePkceWithAuthorizationCodeGrant": True,
            },
            generate_unique_id_function=lambda route: f"{route.tags[0]}_{route.name}",
            title="Dirac",
            lifespan=lifespan,
        )

    def openapi(self, *args, **kwargs):
        if not self.openapi_schema:
            super().openapi(*args, **kwargs)
            for _, method_item in self.openapi_schema.get("paths").items():
                for _, param in method_item.items():
                    responses = param.get("responses")
                    # remove 422 response, also can remove other status code
                    if "422" in responses:
                        del responses["422"]
        return self.openapi_schema


def create_app_inner(
    jobs: JobsSecrets | None, config: ConfigSecrets | None, auth: AuthSecrets | None
) -> DiracFastAPI:
    app = DiracFastAPI()

    # Add routers
    if jobs:
        app.include_router(
            job_manager_router,
            prefix="/jobs",
            dependencies=[Depends(verify_dirac_token)],
        )
        app.lifetime_functions.append(jobs.db.engine_context)

    if config:
        app.include_router(
            configuration_router,
            prefix="/config",
            dependencies=[Depends(verify_dirac_token)],
        )

    if auth:
        app.include_router(auth_router, prefix="/auth")
        app.lifetime_functions.append(auth.db.engine_context)

    app.include_router(well_known_router)

    # Add exception handlers
    app.add_exception_handler(DiracError, dirac_error_handler)
    app.add_exception_handler(DiracHttpResponse, http_response_handler)

    return app


def create_app() -> DiracFastAPI:
    get_secrets.cache_clear()  # type: ignore
    secrets = get_secrets()

    for env_file in dotenv_files_from_environment("DIRACX_SECRET_DOTENV"):
        if not dotenv.load_dotenv(env_file):
            raise NotImplementedError(f"Could not load dotenv file {env_file}")

    return create_app_inner(**dict(secrets._iter()))


def dirac_error_handler(request: Request, exc: DiracError) -> Response:
    return JSONResponse(
        status_code=exc.http_status_code, content={"detail": exc.detail}
    )


def http_response_handler(request: Request, exc: DiracHttpResponse) -> Response:
    return JSONResponse(status_code=exc.status_code, content=exc.data)
