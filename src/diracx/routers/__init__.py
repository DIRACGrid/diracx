from __future__ import annotations

import inspect
import logging
import os
from functools import partial
from typing import AsyncContextManager, AsyncGenerator, Iterable, TypeVar

import dotenv
import starlette
from fastapi import Depends, Request
from fastapi.dependencies.models import Dependant
from fastapi.responses import JSONResponse, Response
from fastapi.routing import APIRoute
from pydantic import parse_raw_as

from diracx.core.exceptions import DiracError, DiracHttpResponse
from diracx.core.extensions import select
from diracx.core.utils import dotenv_files_from_environment
from diracx.db.utils import BaseDB

from ..core.settings import ServiceSettingsBase
from .auth import verify_dirac_token
from .fastapi_classes import DiracFastAPI, DiracxRouter

T = TypeVar("T")
T2 = TypeVar("T2", bound=AsyncContextManager)
logger = logging.getLogger(__name__)


# Rules:
# All routes must have tags (needed for auto gen of client)
# Form headers must have a description (autogen)
# methods name should follow the generate_unique_id_function pattern


def create_app_inner(
    *all_service_settings: ServiceSettingsBase,
    database_urls: dict[str, str],
) -> DiracFastAPI:
    app = DiracFastAPI()

    class_to_settings: dict[type[ServiceSettingsBase], ServiceSettingsBase] = {}
    for service_settings in all_service_settings:
        if type(service_settings) in class_to_settings:
            raise NotImplementedError(f"{type(service_settings)} has been reused")
        class_to_settings[type(service_settings)] = service_settings
        app.dependency_overrides[type(service_settings).create] = partial(
            lambda x: x, service_settings
        )

    required_db_classes = set()
    for entry_point in select(group="diracx.services"):
        router: DiracxRouter = entry_point.load()
        prefix = f"/{entry_point.name}"

        # Apply the settings cache
        if not settings_are_available(router, prefix, class_to_settings):
            continue

        for route in router.routes:
            if not isinstance(route, APIRoute):
                continue
            required_db_classes |= set(
                find_dependents(route.dependant.dependencies, BaseDB)
            )

        app.include_router(
            router,
            prefix=f"/{entry_point.name}",
            tags=[entry_point.name],
            dependencies=[Depends(verify_dirac_token)]
            if router.diracx_require_auth
            else [],
        )

    for db_name, db_url in database_urls.items():
        db_classes: list[type[BaseDB]] = [
            entry_point.load()
            for entry_point in select(group="diracx.dbs", name=db_name)
        ]
        if not any(c in required_db_classes for c in db_classes):
            continue
        db = db_classes[0](db_url=db_url)
        app.lifetime_functions.append(db.engine_context)
        for db_class in db_classes:
            assert db_class.transaction not in app.dependency_overrides
            app.dependency_overrides[db_class.transaction] = partial(db_transaction, db)

    # Add exception handlers
    app.add_exception_handler(DiracError, dirac_error_handler)
    app.add_exception_handler(DiracHttpResponse, http_response_handler)

    return app


def create_app() -> DiracFastAPI:
    for env_file in dotenv_files_from_environment("DIRACX_SERVICE_DOTENV"):
        if not dotenv.load_dotenv(env_file):
            raise NotImplementedError(f"Could not load dotenv file {env_file}")

    # Load all available routers
    settings_classes = set()
    for entry_point in select(group="diracx.services"):
        router: starlette.routing.Router = entry_point.load()
        settings_classes |= set(find_dependents(router, ServiceSettingsBase))

    all_service_settings = []
    for settings_class in settings_classes:
        env_prefix = settings_class.__config__.env_prefix
        enabled = parse_raw_as(bool, os.environ.get(f"{env_prefix}ENABLED", "true"))
        if enabled:
            all_service_settings.append(settings_class())

    return create_app_inner(
        *all_service_settings, database_urls=BaseDB.available_urls()
    )


def dirac_error_handler(request: Request, exc: DiracError) -> Response:
    return JSONResponse(
        status_code=exc.http_status_code, content={"detail": exc.detail}
    )


def http_response_handler(request: Request, exc: DiracHttpResponse) -> Response:
    return JSONResponse(status_code=exc.status_code, content=exc.data)


def find_dependents(
    obj: starlette.routing.Router | Iterable[Dependant], cls: type[T]
) -> Iterable[type[T]]:
    if isinstance(obj, starlette.routing.Router):
        for route in obj.routes:
            if isinstance(route, APIRoute):
                yield from find_dependents(route.dependant.dependencies, cls)
        return

    for dependency in obj:
        bound_class = getattr(dependency.call, "__self__", None)
        if inspect.isclass(bound_class) and issubclass(bound_class, cls):
            yield bound_class
        yield from find_dependents(dependency.dependencies, cls)


async def db_transaction(db: T2) -> AsyncGenerator[T2, None]:
    async with db:
        yield db


def settings_are_available(
    router: starlette.routing.Router,
    prefix: str,
    available_settings_classes: Iterable[type[ServiceSettingsBase]],
):
    for cls in find_dependents(router, ServiceSettingsBase):
        if cls not in available_settings_classes:
            logger.info(f"Not enabling {prefix} as it requires {cls}")
            return False
    return True
