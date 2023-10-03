from __future__ import annotations

import inspect
import logging
import os
from functools import partial
from typing import Any, AsyncContextManager, AsyncGenerator, Iterable, TypeVar

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
from diracx.db.os.utils import BaseOSDB
from diracx.db.sql.utils import BaseSQLDB

from ..core.settings import ServiceSettingsBase
from .auth import verify_dirac_access_token
from .fastapi_classes import DiracFastAPI, DiracxRouter

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
    os_database_conn_kwargs: dict[str, Any],
    config_source: ConfigSource,
) -> DiracFastAPI:
    app = DiracFastAPI()

    # Find which settings classes are available and add them to dependency_overrides
    available_settings_classes: set[type[ServiceSettingsBase]] = set()
    for service_settings in all_service_settings:
        cls = type(service_settings)
        assert cls not in available_settings_classes
        available_settings_classes.add(cls)
        app.lifetime_functions.append(service_settings.lifetime_function)
        app.dependency_overrides[cls.create] = partial(lambda x: x, service_settings)

    # Override the configuration source
    app.dependency_overrides[ConfigSource.create] = config_source.read_config

    # Add the SQL DBs to the application
    available_sql_db_classes: set[type[BaseSQLDB]] = set()
    for db_name, db_url in database_urls.items():
        sql_db_classes = BaseSQLDB.available_implementations(db_name)
        # The first DB is the highest priority one
        sql_db = sql_db_classes[0](db_url=db_url)
        app.lifetime_functions.append(sql_db.engine_context)
        # Add overrides for all the DB classes, including those from extensions
        # This means vanilla DiracX routers get an instance of the extension's DB
        for sql_db_class in sql_db_classes:
            assert sql_db_class.transaction not in app.dependency_overrides
            available_sql_db_classes.add(sql_db_class)
            app.dependency_overrides[sql_db_class.transaction] = partial(
                db_transaction, sql_db
            )

    # Add the OpenSearch DBs to the application
    available_os_db_classes: set[type[BaseOSDB]] = set()
    for db_name, connection_kwargs in os_database_conn_kwargs.items():
        os_db_classes = BaseOSDB.available_implementations(db_name)
        # The first DB is the highest priority one
        os_db = os_db_classes[0](connection_kwargs=connection_kwargs)
        app.lifetime_functions.append(os_db.client_context)
        # Add overrides for all the DB classes, including those from extensions
        # This means vanilla DiracX routers get an instance of the extension's DB
        for os_db_class in os_db_classes:
            assert os_db_class.session not in app.dependency_overrides
            available_os_db_classes.add(os_db_class)
            app.dependency_overrides[os_db_class.session] = partial(db_session, os_db)

    # Load the requested routers
    routers: dict[str, APIRouter] = {}
    # The enabled systems must be sorted to ensure the openapi.json is deterministic
    # Without this AutoREST generates different client sources for each ordering
    for system_name in sorted(enabled_systems):
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
        missing_sql_dbs = (
            set(find_dependents(router, BaseSQLDB)) - available_sql_db_classes
        )
        if missing_sql_dbs:
            raise NotImplementedError(
                f"Cannot enable {system_name=} as it requires {missing_sql_dbs=}"
            )
        missing_os_dbs = (
            set(find_dependents(router, BaseOSDB))  # type: ignore[type-abstract]
            - available_os_db_classes
        )
        if missing_os_dbs:
            raise NotImplementedError(
                f"Cannot enable {system_name=} as it requires {missing_os_dbs=}"
            )

        # Add the router to the application
        dependencies = []
        if isinstance(router, DiracxRouter) and router.diracx_require_auth:
            dependencies.append(Depends(verify_dirac_access_token))
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
        logger.debug("Loading dotenv file: %s", env_file)
        if not dotenv.load_dotenv(env_file):
            raise NotImplementedError(f"Could not load dotenv file {env_file}")

    # Load all available routers
    enabled_systems = set()
    settings_classes = set()
    for entry_point in select_from_extension(group="diracx.services"):
        env_var = f"DIRACX_SERVICE_{entry_point.name.upper()}_ENABLED"
        enabled = parse_raw_as(bool, os.environ.get(env_var, "true"))
        logger.debug("Found service %r: enabled=%s", entry_point, enabled)
        if not enabled:
            continue
        router: APIRouter = entry_point.load()
        enabled_systems.add(entry_point.name)
        dependencies = set(find_dependents(router, ServiceSettingsBase))
        logger.debug("Found dependencies for %r: enabled=%s", entry_point, dependencies)
        settings_classes |= dependencies

    # Load settings classes required by the routers
    all_service_settings = [settings_class() for settings_class in settings_classes]

    return create_app_inner(
        enabled_systems=enabled_systems,
        all_service_settings=all_service_settings,
        database_urls=BaseSQLDB.available_urls(),
        os_database_conn_kwargs=BaseOSDB.available_urls(),
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


async def db_session(db: T2) -> AsyncGenerator[T2, None]:
    yield db
