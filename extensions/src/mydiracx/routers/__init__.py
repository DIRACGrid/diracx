from __future__ import annotations

import inspect
import logging
import os
from functools import partial
from typing import AsyncContextManager, AsyncGenerator, Iterable, TypeVar

import dotenv
from fastapi import APIRouter, Depends, Request
from fastapi.dependencies.models import Dependant
from fastapi.responses import JSONResponse, Response
from fastapi.routing import APIRoute
from pydantic import parse_raw_as

from diracx.core.config import ConfigSource
from diracx.core.exceptions import DiracError, DiracHttpResponse
from diracx.core.extensions import select_from_extension
from diracx.core.utils import dotenv_files_from_environment
from diracx.db.utils import BaseDB

from diracx.core.settings import ServiceSettingsBase
from diracx.routers.auth import verify_dirac_token
from diracx.routers.fastapi_classes import DiracFastAPI, DiracxRouter

T = TypeVar("T")
T2 = TypeVar("T2", bound=AsyncContextManager)
logger = logging.getLogger(__name__)


# Rules:
# All routes must have tags (needed for auto gen of client)
# Form headers must have a description (autogen)
# methods name should follow the generate_unique_id_function pattern


def create_app_inner(
    *,
    enabled_systems: set[str],
    all_service_settings: Iterable[ServiceSettingsBase],
    database_urls: dict[str, str],
    config_source: ConfigSource,
) -> DiracFastAPI:
    app = DiracFastAPI()

    # Find which settings classes are available and add them to dependency_overrides
    available_settings_classes: set[type[ServiceSettingsBase]] = set()
    for service_settings in all_service_settings:
        cls = type(service_settings)
        assert cls not in available_settings_classes
        available_settings_classes.add(cls)
        app.dependency_overrides[cls.create] = partial(lambda x: x, service_settings)

    # Override the configuration source
    app.dependency_overrides[ConfigSource.create] = config_source.read_config

    # Add the DBs to the application
    available_db_classes: set[type[BaseDB]] = set()
    for db_name, db_url in database_urls.items():
        db_classes: list[type[BaseDB]] = [
            entry_point.load()
            for entry_point in select_from_extension(group="diracx.dbs", name=db_name)
        ]
        assert db_classes, f"Could not find {db_name=}"
        # The first DB is the highest priority one
        db = db_classes[0](db_url=db_url)
        app.lifetime_functions.append(db.engine_context)
        # Add overrides for all the DB classes, including those from extensions
        # This means vanilla DiracX routers get an instance of the extension's DB
        for db_class in db_classes:
            assert db_class.transaction not in app.dependency_overrides
            available_db_classes.add(db_class)
            app.dependency_overrides[db_class.transaction] = partial(db_transaction, db)

    # Load the requested routers
    routers: dict[str, APIRouter] = {}
    for system_name in enabled_systems:
        assert system_name not in routers
        for entry_point in select_from_extension(
            group="diracx.services", name=system_name
        ):
            routers[system_name] = entry_point.load()
            break
        else:
            raise NotImplementedError(f"Could not find {system_name=}")

    # Add routers ensuring that all the required settings are available
    for system_name, router in routers.items():
        # Ensure required settings are available
        for cls in find_dependents(router, ServiceSettingsBase):
            if cls not in available_settings_classes:
                raise NotImplementedError(
                    f"Cannot enable {system_name=} as it requires {cls=}"
                )

        # Ensure required DBs are available
        missing_dbs = set(find_dependents(router, BaseDB)) - available_db_classes
        if missing_dbs:
            raise NotImplementedError(
                f"Cannot enable {system_name=} as it requires {missing_dbs=}"
            )

        # Add the router to the application
        dependencies = []
        if isinstance(router, DiracxRouter) and router.diracx_require_auth:
            dependencies.append(Depends(verify_dirac_token))
        app.include_router(
            router,
            prefix=f"/{system_name}",
            tags=[system_name],
            dependencies=dependencies,
        )

    # Add exception handlers
    app.add_exception_handler(DiracError, dirac_error_handler)
    app.add_exception_handler(DiracHttpResponse, http_response_handler)

    return app


def create_app() -> DiracFastAPI:
    """Load settings from the environment and create the application object"""
    for env_file in dotenv_files_from_environment("DIRACX_SERVICE_DOTENV"):
        if not dotenv.load_dotenv(env_file):
            raise NotImplementedError(f"Could not load dotenv file {env_file}")

    # Load all available routers
    enabled_systems = set()
    settings_classes = set()
    for entry_point in select_from_extension(group="diracx.services"):
        env_var = f"DIRACX_SERVICE_{entry_point.name.upper()}_ENABLED"
        enabled = parse_raw_as(bool, os.environ.get(env_var, "true"))
        print(f"Found service {entry_point}: {enabled=}")
        if not enabled:
            continue
        router: APIRouter = entry_point.load()
        enabled_systems.add(entry_point.name)
        dependencies = set(find_dependents(router, ServiceSettingsBase))
        print(f"Found dependencies: {dependencies}")
        settings_classes |= dependencies

    # Load settings classes required by the routers
    all_service_settings = [settings_class() for settings_class in settings_classes]

    return create_app_inner(
        enabled_systems=enabled_systems,
        all_service_settings=all_service_settings,
        database_urls=BaseDB.available_urls(),
        config_source=ConfigSource.create(),
    )


def dirac_error_handler(request: Request, exc: DiracError) -> Response:
    return JSONResponse(
        status_code=exc.http_status_code, content={"detail": exc.detail}
    )


def http_response_handler(request: Request, exc: DiracHttpResponse) -> Response:
    return JSONResponse(status_code=exc.status_code, content=exc.data)


def find_dependents(
    obj: APIRouter | Iterable[Dependant], cls: type[T]
) -> Iterable[type[T]]:
    if isinstance(obj, APIRouter):
        # TODO: Support dependencies of the router itself
        # yield from find_dependents(obj.dependencies, cls)
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
