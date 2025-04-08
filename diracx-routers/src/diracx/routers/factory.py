"""Logic for creating and configuring the FastAPI application."""

from __future__ import annotations

import inspect
import logging
import os
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable, Sequence
from functools import partial
from http import HTTPStatus
from importlib.metadata import EntryPoint, EntryPoints, entry_points
from logging import Formatter, StreamHandler
from typing import (
    Any,
    TypeVar,
    cast,
)

import dotenv
from cachetools import TTLCache
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, status
from fastapi.dependencies.models import Dependant
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from fastapi.routing import APIRoute
from packaging.version import InvalidVersion, parse
from pydantic import TypeAdapter
from starlette.middleware.base import BaseHTTPMiddleware
from uvicorn.logging import AccessFormatter, DefaultFormatter

from diracx.core.config import ConfigSource
from diracx.core.exceptions import DiracError, DiracHttpResponseError
from diracx.core.extensions import select_from_extension
from diracx.core.settings import ServiceSettingsBase
from diracx.core.utils import dotenv_files_from_environment
from diracx.db.exceptions import DBUnavailableError
from diracx.db.os.utils import BaseOSDB
from diracx.db.sql.utils import BaseSQLDB
from diracx.routers.access_policies import BaseAccessPolicy, check_permissions

from .fastapi_classes import DiracFastAPI, DiracxRouter
from .otel import instrument_otel
from .utils.users import verify_dirac_access_token

T = TypeVar("T")
T2 = TypeVar("T2", bound=BaseSQLDB | BaseOSDB)


logger = logging.getLogger(__name__)
logger_422 = logger.getChild("debug.422.errors")


DIRACX_MIN_CLIENT_VERSION = "0.0.1a1"

###########################################3


def configure_logger():
    """Configure the console logger.

    Access logs come from uvicorn, which configure its logger in a certain way
    (https://github.com/tiangolo/fastapi/discussions/7457)
    This method adds a timestamp to the uvicorn output,
    and define a console handler for all the diracx loggers
    We cannot configure just the root handler, as uvicorn
    attaches handler to the `uvicorn` logger
    """
    diracx_handler = StreamHandler()
    diracx_handler.setFormatter(Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logging.getLogger("diracx").addHandler(diracx_handler)
    logging.getLogger("diracx").setLevel("INFO")

    # Recreate the formatters for the uvicorn loggers adding the timestamp
    uvicorn_access_logger = logging.getLogger("uvicorn.access")
    try:
        previous_fmt = uvicorn_access_logger.handlers[0].formatter._fmt
        new_format = f"%(asctime)s - {previous_fmt}"
        uvicorn_access_logger.handlers[0].setFormatter(AccessFormatter(new_format))
    # There may not be any handler defined, like in the CI
    except IndexError:
        pass

    uvicorn_logger = logging.getLogger("uvicorn")
    try:
        previous_fmt = uvicorn_logger.handlers[0].formatter._fmt
        new_format = f"%(asctime)s - {previous_fmt}"
        uvicorn_logger.handlers[0].setFormatter(DefaultFormatter(new_format))
    # There may not be any handler defined, like in the CI
    except IndexError:
        pass


# Rules:
# All routes must have tags (needed for auto gen of client)
# Form headers must have a description (autogen)
# methods name should follow the generate_unique_id_function pattern
# All routes should have a policy mechanism


def create_app_inner(
    *,
    enabled_systems: set[str],
    all_service_settings: Iterable[ServiceSettingsBase],
    database_urls: dict[str, str],
    os_database_conn_kwargs: dict[str, Any],
    config_source: ConfigSource,
    all_access_policies: dict[str, Sequence[BaseAccessPolicy]],
) -> DiracFastAPI:
    """This method does the heavy lifting work of putting all the pieces together.

    When starting the application normaly, this method is called by create_app,
    and the values of the parameters are taken from environment variables or
    entrypoints.

    When running tests, the parameters are mocks or test settings.

    We rely on the dependency_override mechanism to implement
    the actual behavior we are interested in for settings, DBs or policy.
    This allows an extension to override any of these components


    :param enabled_system:
         this contains the name of all the routers we have to load
    :param all_service_settings:
        list of instance of each Settings type required
    :param database_urls:
        dict <db_name: url>. When testing, sqlite urls are used
    :param os_database_conn_kwargs:
        <db_name:dict> containing all the parameters the OpenSearch client takes
    :param config_source:
        Source of the configuration to use
    :param all_access_policies:
        <policy_name: [implementations]>


    """
    app = DiracFastAPI()

    # Find which settings classes are available and add them to dependency_overrides
    # We use a single instance of each Setting classes for performance reasons,
    # since it avoids recreating a pydantic model every time
    # We add the Settings lifetime_function to the application lifetime_function,
    # Please see ServiceSettingsBase for more details

    available_settings_classes: set[type[ServiceSettingsBase]] = set()

    for service_settings in all_service_settings:
        cls = type(service_settings)
        assert cls not in available_settings_classes
        available_settings_classes.add(cls)
        app.lifetime_functions.append(service_settings.lifetime_function)
        # We always return the same setting instance for perf reasons
        app.dependency_overrides[cls.create] = partial(lambda x: x, service_settings)

    # Override the ConfigSource.create by the actual reading of the config
    app.dependency_overrides[ConfigSource.create] = config_source.read_config

    all_access_policies_used = {}

    for access_policy_name, access_policy_classes in all_access_policies.items():

        # The first AccessPolicy is the highest priority one
        access_policy_used = access_policy_classes[0].policy
        all_access_policies_used[access_policy_name] = access_policy_classes[0]

        # app.lifetime_functions.append(access_policy.lifetime_function)
        # Add overrides for all the AccessPolicy classes, including those from extensions
        # This means vanilla DiracX routers get an instance of the extension's AccessPolicy
        for access_policy_class in access_policy_classes:
            # Here we do not check that access_policy_class.check is
            # not already in the dependency_overrides becaue the same
            # policy could be used for multiple purpose
            # (e.g. open access)
            # assert access_policy_class.check not in app.dependency_overrides
            app.dependency_overrides[access_policy_class.check] = partial(
                check_permissions,
                policy=access_policy_used,
                policy_name=access_policy_name,
            )

    app.dependency_overrides[BaseAccessPolicy.all_used_access_policies] = (
        lambda: all_access_policies_used
    )

    fail_startup = True
    # Add the SQL DBs to the application
    available_sql_db_classes: set[type[BaseSQLDB]] = set()

    for db_name, db_url in database_urls.items():

        try:
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

            # At least one DB works, so we do not fail the startup
            fail_startup = False
        except Exception:
            logger.exception("Failed to initialize DB %s", db_name)

    if fail_startup:
        raise Exception("No SQL database could be initialized, aborting")

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
            app.dependency_overrides[os_db_class.session] = partial(
                db_transaction, os_db
            )

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
        # Most routers are mounted under /api/<system_name>
        path_root = getattr(router, "diracx_path_root", "/api")
        app.include_router(
            router,
            prefix=f"{path_root}/{system_name}",
            tags=[system_name],
            dependencies=dependencies,
        )

    # Add exception handlers
    # We need to cast because callables are contravariant and we define our exception handlers
    # with a subclass of Exception (https://mypy.readthedocs.io/en/latest/generics.html#variance-of-generic-types)
    handler_signature = Callable[[Request, Exception], Response | Awaitable[Response]]
    app.add_exception_handler(DiracError, cast(handler_signature, dirac_error_handler))
    app.add_exception_handler(
        DiracHttpResponseError, cast(handler_signature, http_response_handler)
    )
    app.add_exception_handler(
        DBUnavailableError, cast(handler_signature, route_unavailable_error_hander)
    )
    app.add_exception_handler(
        RequestValidationError, cast(handler_signature, validation_error_handler)
    )

    # TODO: remove the CORSMiddleware once we figure out how to launch
    # diracx and diracx-web under the same origin
    origins = [
        "http://localhost:8000",
        "http://localhost:3000",
    ]

    app.add_middleware(ClientMinVersionCheckMiddleware)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    configure_logger()
    instrument_otel(app)

    return app


def create_app() -> DiracFastAPI:
    """Load settings from the environment and create the application object.

    The configuration may be placed in .env files pointed to by
    environment variables DIRACX_SERVICE_DOTENV.
    They can be followed by "_X" where X is a number, and the order
    is respected.

    We then loop over all the diracx.services definitions.
    A specific route can be disabled with an environment variable
    DIRACX_SERVICE_<name>_ENABLED=false
    For each of the enabled route, we inspect which Setting classes
    are needed.

    We attempt to load each setting classes to make sure that the
    settings are correctly defined.
    """
    for env_file in dotenv_files_from_environment("DIRACX_SERVICE_DOTENV"):
        logger.debug("Loading dotenv file: %s", env_file)
        if not dotenv.load_dotenv(env_file):
            raise NotImplementedError(f"Could not load dotenv file {env_file}")

    # Load all available routers
    enabled_systems = set()
    settings_classes = set()
    for entry_point in select_from_extension(group="diracx.services"):
        env_var = f"DIRACX_SERVICE_{entry_point.name.upper()}_ENABLED"
        enabled = TypeAdapter(bool).validate_json(os.environ.get(env_var, "true"))
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

    # Find all the access policies

    available_access_policy_names = {
        entry_point.name
        for entry_point in select_from_extension(group="diracx.access_policies")
    }

    all_access_policies = {}

    for access_policy_name in available_access_policy_names:

        access_policy_classes = BaseAccessPolicy.available_implementations(
            access_policy_name
        )
        all_access_policies[access_policy_name] = access_policy_classes

    return create_app_inner(
        enabled_systems=enabled_systems,
        all_service_settings=all_service_settings,
        database_urls=BaseSQLDB.available_urls(),
        os_database_conn_kwargs=BaseOSDB.available_urls(),
        config_source=ConfigSource.create(),
        all_access_policies=all_access_policies,
    )


def dirac_error_handler(request: Request, exc: DiracError) -> Response:
    return JSONResponse(
        status_code=exc.http_status_code,
        content={"detail": exc.detail},
        headers=exc.http_headers,
    )


def http_response_handler(request: Request, exc: DiracHttpResponseError) -> Response:
    return JSONResponse(status_code=exc.status_code, content=exc.data)


def route_unavailable_error_hander(request: Request, exc: DBUnavailableError):
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        headers={"Retry-After": "10"},
        content={"detail": str(exc.args)},
    )


async def validation_error_handler(request: Request, exc: RequestValidationError):
    logger_422.warning(
        "Got validation error: %s in %s %s with body %r",
        exc.errors(),
        request.method,
        request.url,
        await request.body(),
        # TODO: It would be nicer to do:
        # extra={
        #     "request": {
        #         "method": request.method,
        #         "url": str(request.url),
        #         "body": await request.body(),
        #         "headers": request.headers, should probably be an allowlist
        #     }
        # },
    )
    return await request_validation_exception_handler(request, exc)


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


_db_alive_cache: TTLCache = TTLCache(maxsize=1024, ttl=10)


async def is_db_unavailable(db: BaseSQLDB | BaseOSDB) -> str:
    """Cache the result of pinging the DB
    (exceptions are not cachable).
    """
    if db not in _db_alive_cache:
        try:
            await db.ping()
            _db_alive_cache[db] = ""

        except DBUnavailableError as e:
            _db_alive_cache[db] = e.args[0]

    return _db_alive_cache[db]


async def db_transaction(db: T2) -> AsyncGenerator[T2]:
    """Initiate a DB transaction."""
    # Entering the context already triggers a connection to the DB
    # that may fail
    async with db:
        # Check whether the connection still works before executing the query
        if reason := await is_db_unavailable(db):
            raise DBUnavailableError(reason)
        yield db


class ClientMinVersionCheckMiddleware(BaseHTTPMiddleware):
    """Custom FastAPI middleware to verify that
    the client has the required minimum version.
    """

    def __init__(self, app: FastAPI):
        super().__init__(app)
        self.min_client_version = get_min_client_version()
        self.parsed_min_client_version = parse(self.min_client_version)

    async def dispatch(self, request: Request, call_next) -> Response:
        client_version = request.headers.get("DiracX-Client-Version")

        try:
            if client_version and self.is_version_too_old(client_version):
                # When comes from Swagger or Web, there is no client version header.
                # This is not managed here.

                raise HTTPException(
                    status_code=HTTPStatus.UPGRADE_REQUIRED,
                    detail=f"Client version ({client_version})"
                    f"not recent enough (>= {self.min_client_version})."
                    "Upgrade.",
                )
        except HTTPException as exc:
            # Return a JSONResponse because the HTTPException
            # is not handled nicely in the middleware
            logger.error("Error checking client version %s", client_version)
            return JSONResponse(
                status_code=exc.status_code,
                content={"detail": exc.detail},
            )
        # If the version is not given
        except Exception:  # noqa: S110
            pass

        response = await call_next(request)
        return response

    def is_version_too_old(self, client_version: str) -> bool | None:
        """Verify that client version is ge than min."""
        try:
            return parse(client_version) < self.parsed_min_client_version
        except InvalidVersion as iv_exc:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail=f"Invalid version string: '{client_version}'",
            ) from iv_exc


def get_min_client_version():
    """Extracting min client version from entry_points and searching for extension."""
    matched_entry_points: EntryPoints = entry_points(group="diracx.min_client_version")
    # Searching for an extension:
    entry_points_dict: dict[str, EntryPoint] = {
        ep.name: ep for ep in matched_entry_points
    }
    for ep_name, ep in entry_points_dict.items():
        if ep_name != "diracx":
            return ep.load()

    # Taking diracx if no extension:
    if "diracx" in entry_points_dict:
        return entry_points_dict["diracx"].load()
