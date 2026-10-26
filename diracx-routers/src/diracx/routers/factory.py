"""Create and configure the DiracX FastAPI application.

The factory loads enabled service routers, resolves their settings and
database dependencies, installs extension overrides, and registers middleware
and exception handlers.
"""

from __future__ import annotations

__all__ = ["DIRACX_MIN_CLIENT_VERSION", "create_app", "create_app_inner"]

import inspect
import logging
import math
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable, Sequence
from functools import partial
from http import HTTPStatus
from importlib.metadata import EntryPoint, EntryPoints, entry_points
from typing import Any, TypeVar, cast

from cachetools import TTLCache
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from fastapi.dependencies.models import Dependant
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from fastapi.routing import APIRoute, iter_route_contexts
from packaging.version import InvalidVersion, parse
from starlette.middleware.base import BaseHTTPMiddleware

from diracx.core.config import ConfigSource
from diracx.core.exceptions import DiracError, DocumentUpsertError, NotReadyError
from diracx.core.extensions import DiracEntryPoint, select_from_extension
from diracx.core.logs import configure_logging
from diracx.core.settings import FactorySettings, ServiceSettingsBase
from diracx.core.sources import AsyncCacheableSource
from diracx.db.exceptions import DBUnavailableError
from diracx.db.os.utils import BaseOSDB
from diracx.db.sql.utils import BaseSQLDB
from diracx.routers.access_policies import BaseAccessPolicy, check_permissions

from .fastapi_classes import DiracFastAPI, DiracxRouter
from .otel import instrument_otel, record_client_version
from .utils.users import verify_dirac_access_token

T = TypeVar("T")
T2 = TypeVar("T2", bound=BaseSQLDB | BaseOSDB)


logger = logging.getLogger(__name__)
logger_422 = logger.getChild("debug.422.errors")


DIRACX_MIN_CLIENT_VERSION = "0.0.1a1"

###########################################3


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
    """Assemble the application from resolved settings and dependencies.

    ``create_app`` supplies values discovered from the environment and
    extension entry points. Tests may call this function directly with mocks
    or test settings. Dependency overrides provide the configured settings,
    databases, cacheable sources, and access policies to routes, which allows
    an extension to override any of these components.

    Args:
        enabled_systems: Names of service routers to load and mount.
        all_service_settings: One instance of each settings class required by
            the enabled services.
        database_urls: Database name-to-URL mapping for SQL databases. When
            testing, sqlite URLs are used.
        os_database_conn_kwargs: Database name-to-connection options for
            OpenSearch, containing all the parameters the OpenSearch client
            takes.
        config_source: Source used to load the DiracX configuration.
        all_access_policies: Access-policy names mapped to implementations in
            priority order.

    Returns:
        A configured FastAPI application.

    Raises:
        Exception: If no SQL database can be initialized.
        NotImplementedError: If an enabled service cannot be found or requires
            a settings class, database, or cacheable source that is unavailable.
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
    # Mark it as non-blocking so we can serve 503 errors while waiting for the config
    app.dependency_overrides[ConfigSource.create] = config_source.read_non_blocking

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
            # not already in the dependency_overrides because the same
            # policy could be used for multiple purpose
            # (e.g. open access)
            # assert access_policy_class.check not in app.dependency_overrides
            app.dependency_overrides[access_policy_class.check] = partial(
                check_permissions,
                policy=access_policy_used,
                policy_name=access_policy_name,
            )

    app.dependency_overrides[BaseAccessPolicy.all_used_access_policies] = lambda: (
        all_access_policies_used
    )

    fail_startup = True
    # Add the SQL DBs to the application
    available_sql_db_classes: set[type[BaseSQLDB]] = set()
    sql_db_instances: dict[type[BaseSQLDB], BaseSQLDB] = {}

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
                sql_db_instances[sql_db_class] = sql_db

                app.dependency_overrides[sql_db_class.transaction] = partial(
                    db_transaction, sql_db
                )
                app.dependency_overrides[sql_db_class.no_transaction] = partial(
                    db_no_transaction, sql_db
                )

            # At least one DB works, so we do not fail the startup
            fail_startup = False
        except Exception:
            logger.exception("Failed to initialize DB %s", db_name)

    if fail_startup:
        raise Exception("No SQL database could be initialized, aborting")

    # Instantiate the cacheable sources and override their create methods,
    # mirroring the SQL DB wiring above. A single instance is used for each
    # source name so that its caches persist across requests. The instance is
    # made from the highest priority class but, as for the DBs, the override
    # is registered for every implementation class so that vanilla DiracX
    # routers get an instance of the extension's source.
    source_classes_by_name: dict[str, list[type[AsyncCacheableSource]]] = {}
    for entry_point in select_from_extension(group=DiracEntryPoint.CACHEABLE_SOURCES):
        # Entry points are yielded in priority order, highest first
        source_classes_by_name.setdefault(entry_point.name, []).append(
            entry_point.load()
        )
    for source_name, source_classes in source_classes_by_name.items():
        source_db = sql_db_instances.get(source_classes[0].db_class)
        if source_db is None:
            logger.warning(
                "Cannot wire cacheable source %s: %s is not available",
                source_name,
                source_classes[0].db_class.__name__,
            )
            continue
        # The base class does not declare the ``db`` argument as it is only
        # part of the contract between the factory and the concrete sources
        source = source_classes[0](db=source_db)  # type: ignore[call-arg]
        for source_cls in source_classes:
            assert source_cls.create not in app.dependency_overrides
            app.dependency_overrides[source_cls.create] = source.read

    # Add the OpenSearch DBs to the application
    os_global_prefix = FactorySettings().os_global_prefix
    available_os_db_classes: set[type[BaseOSDB]] = set()
    for db_name, connection_kwargs in os_database_conn_kwargs.items():
        os_db_classes = BaseOSDB.available_implementations(db_name)
        # The first DB is the highest priority one
        os_db = os_db_classes[0](
            connection_kwargs=connection_kwargs, global_prefix=os_global_prefix
        )
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
            group=DiracEntryPoint.SERVICES, name=system_name
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

        # Ensure required cacheable sources have been wired, i.e. that the
        # database they read from is available
        for source_cls in find_dependents(
            router,
            AsyncCacheableSource,  # type: ignore[type-abstract]
        ):
            if source_cls.create not in app.dependency_overrides:
                raise NotImplementedError(
                    f"Cannot enable {system_name=} as it requires {source_cls=}"
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
        DBUnavailableError, cast(handler_signature, route_unavailable_error_hander)
    )
    app.add_exception_handler(
        RequestValidationError, cast(handler_signature, validation_error_handler)
    )
    app.add_exception_handler(
        NotReadyError, cast(handler_signature, route_unavailable_error_hander)
    )
    app.add_exception_handler(
        DocumentUpsertError, cast(handler_signature, document_upsert_error_handler)
    )

    # TODO: remove the CORSMiddleware once we figure out how to launch
    # diracx and diracx-web under the same origin
    origins = [
        "http://localhost:8000",
        "http://localhost:3000",
        "https://localhost:8000",
        "https://localhost:3000",
    ]

    app.add_middleware(ClientMinVersionCheckMiddleware)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["Content-Range"],
    )

    configure_logging()
    instrument_otel(app)

    return app


def create_app() -> DiracFastAPI:
    """Load enabled services and settings, then create the application.

    Settings can be loaded from dotenv files named by ``DIRACX_SERVICE_DOTENV``
    and its numbered variants. Services are discovered through entry points;
    ``DIRACX_SERVICE_<name>_ENABLED=false`` disables a service.

    The required settings classes and access-policy implementations are
    discovered from the enabled service routers before the application is
    assembled.

    Returns:
        The configured DiracX FastAPI application.

    Raises:
        NotImplementedError: If a required service or access-policy
            implementation cannot be found.
    """
    # Load all available routers
    enabled_systems = set()
    settings_classes = set()
    factory_settings = FactorySettings()
    for entry_point in select_from_extension(group=DiracEntryPoint.SERVICES):
        enabled = factory_settings.enabled_services.get(entry_point.name, True)
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
        for entry_point in select_from_extension(group=DiracEntryPoint.ACCESS_POLICY)
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
    """Convert an unhandled DiracX domain error to a client error response.

    Args:
        request: Request that raised the domain error.
        exc: Domain error not translated by the route.

    Returns:
        A JSON response with status 400 and the error detail.
    """
    return JSONResponse(
        status_code=HTTPStatus.BAD_REQUEST,
        content={"detail": exc.detail},
    )


def document_upsert_error_handler(
    request: Request, exc: DocumentUpsertError
) -> Response:
    """Report a document which could not be indexed as a server error.

    The values which cannot be stored are already rejected by the models, so
    reaching this point means the server is at fault, e.g. a mapping conflict.

    Args:
        request: Request during which document indexing failed.
        exc: Error raised when the document could not be indexed.

    Returns:
        A JSON response with status 500 and the error detail.
    """
    logger.error(
        "500 Internal Server Error: %s (path=%s)",
        exc,
        request.url.path,
        exc_info=True,
    )
    return JSONResponse(
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        content={"detail": exc.detail},
    )


def route_unavailable_error_hander(request: Request, exc: DBUnavailableError):
    """Return a retryable service-unavailable response for a database error.

    Args:
        request: Request that could not be served because a database was unavailable.
        exc: Database availability error to report.

    Returns:
        A JSON response with status 503, a retry header, and the error detail.
    """
    logger.warning(
        "503 Service Unavailable: %s (path=%s)",
        exc,
        request.url.path,
        exc_info=True,
    )
    return JSONResponse(
        status_code=HTTPStatus.SERVICE_UNAVAILABLE,
        headers={"Retry-After": "10"},
        content={"detail": str(exc)},
    )


def _replace_non_finite(obj):
    """Replace non-finite floats recursively with JSON-serializable strings.

    Args:
        obj: Value that may contain nested NaN or infinity values.

    Returns:
        The value with non-finite floats replaced by their representations.
    """
    if isinstance(obj, float) and not math.isfinite(obj):
        return repr(obj)
    if isinstance(obj, dict):
        return {k: _replace_non_finite(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_replace_non_finite(v) for v in obj]
    return obj


async def validation_error_handler(request: Request, exc: RequestValidationError):
    """Log a request validation failure and return its JSON-encoded details.

    Non-finite numbers in the validation errors are converted to strings so
    the error response remains valid JSON.

    Args:
        request: Request that failed validation.
        exc: FastAPI request-validation error.

    Returns:
        A JSON response with status 422 and the validation error details.
    """
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
    # The rejected input is echoed in the error detail and may contain values
    # which cannot be represented in strict JSON, such as NaN
    detail = _replace_non_finite(jsonable_encoder(exc.errors()))
    return JSONResponse(
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        content={"detail": detail},
    )


def find_dependents(
    obj: APIRouter | Iterable[Dependant], cls: type[T]
) -> Iterable[type[T]]:
    """Yield dependency classes used by a router or dependency tree.

    Args:
        obj: Router or FastAPI dependency nodes to inspect recursively.
        cls: Base class whose subclasses should be found.

    Yields:
        Classes bound to dependencies that are subclasses of ``cls``.
    """
    if isinstance(obj, APIRouter):
        # router.routes can be a tree since FastAPI 0.137, iterate over the
        # routes of the whole tree. The dependant of a route context also has
        # the dependencies of the routers and include_router calls above it.
        for route_context in iter_route_contexts(obj.routes):
            if isinstance(route_context.original_route, APIRoute):
                yield from find_dependents(route_context.dependant.dependencies, cls)
        return

    for dependency in obj:
        bound_class = getattr(dependency.call, "__self__", None)
        if inspect.isclass(bound_class) and issubclass(bound_class, cls):
            yield bound_class
        yield from find_dependents(dependency.dependencies, cls)


_db_alive_cache: TTLCache = TTLCache(maxsize=1024, ttl=10)


async def is_db_unavailable(db: BaseSQLDB | BaseOSDB) -> str:
    """Return the cached result of checking database availability.

    Availability results are cached for ten seconds. Successful pings are
    represented by an empty string; database-unavailable errors are returned
    as their message. Other exceptions are not cached.

    Args:
        db: SQL or OpenSearch database to ping.

    Returns:
        Empty string when available, otherwise the cached failure message.
    """
    if db not in _db_alive_cache:
        try:
            await db.ping()
            _db_alive_cache[db] = ""

        except DBUnavailableError as e:
            _db_alive_cache[db] = str(e)

    return _db_alive_cache[db]


async def db_transaction(db: T2) -> AsyncGenerator[T2]:
    """Open a database transaction and verify the connection before yielding.

    Args:
        db: Database whose transaction context should be entered.

    Yields:
        The database instance inside its transaction context.

    Raises:
        DBUnavailableError: If the database ping reports that it is unavailable.
    """
    # Entering the context already triggers a connection to the DB
    # that may fail
    async with db:
        # Check whether the connection still works before executing the query
        if reason := await is_db_unavailable(db):
            raise DBUnavailableError(reason)
        yield db


async def db_no_transaction(db: T2) -> AsyncGenerator[T2]:
    """Yield a database instance without opening a transaction.

    Args:
        db: Database instance to provide to the dependency consumer.

    Yields:
        The unchanged database instance.
    """
    yield db


class ClientMinVersionCheckMiddleware(BaseHTTPMiddleware):
    """Reject requests from clients older than the configured minimum version.

    Requests without a client-version header (e.g. from Swagger or a web
    browser) are allowed through. Requests whose version is older than the
    minimum, or cannot be parsed, are rejected with a 400.
    """

    def __init__(self, app: FastAPI):
        """Initialize middleware and load the minimum supported client version.

        Args:
            app: FastAPI application wrapped by this middleware.
        """
        super().__init__(app)
        self.min_client_version = get_min_client_version()
        self.parsed_min_client_version = parse(self.min_client_version)

    async def dispatch(self, request: Request, call_next) -> Response:
        """Check the client version before dispatching a request.

        Args:
            request: Incoming request to check and forward.
            call_next: ASGI handler that dispatches the request to the next layer.

        Returns:
            A 400 JSON response for an obsolete version, or the downstream response.
        """
        client_version = request.headers.get("DiracX-Client-Version")

        try:
            if client_version and self.is_version_too_old(client_version):
                # When comes from Swagger or Web, there is no client version header.
                # This is not managed here.

                raise HTTPException(
                    status_code=HTTPStatus.BAD_REQUEST,
                    detail=f"Client version {client_version} is not compatible with the server. "
                    f"Upgrade to a version >= {self.min_client_version}.",
                )
        except HTTPException as exc:
            record_client_version(str(request.url), client_version, rejected=True)
            # Return a JSONResponse because the HTTPException
            # is not handled nicely in the middleware
            logger.error("Error checking client version %s", client_version)
            return JSONResponse(
                status_code=exc.status_code,
                content={"detail": exc.detail},
            )
        # Never block a request because the check itself failed unexpectedly
        except Exception:
            logger.debug(
                "Failed to check client version header: %s",
                client_version,
                exc_info=True,
            )

        record_client_version(str(request.url), client_version, rejected=False)
        response = await call_next(request)
        return response

    def is_version_too_old(self, client_version: str) -> bool | None:
        """Compare a client version with the configured minimum.

        Args:
            client_version: Version string supplied by the client.

        Returns:
            Whether the version is older than the minimum supported version.

        Raises:
            HTTPException: If ``client_version`` is not a valid version string (400).
        """
        try:
            return parse(client_version) < self.parsed_min_client_version
        except InvalidVersion as iv_exc:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail=f"Invalid version string: '{client_version}'",
            ) from iv_exc


def get_min_client_version():
    """Load the minimum client version declared by an extension or DiracX.

    The first non-core version entry point takes precedence. If no extension
    declares a version, the core DiracX entry point is used.

    Returns:
        The value loaded from the selected minimum-client-version entry point.
    """
    matched_entry_points: EntryPoints = entry_points(
        group=DiracEntryPoint.MIN_CLIENT_VERSION
    )
    # Searching for an extension:
    entry_points_dict: dict[str, EntryPoint] = {
        ep.name: ep for ep in matched_entry_points
    }
    for ep_name, ep in entry_points_dict.items():
        if ep_name != DiracEntryPoint.CORE:
            return ep.load()

    # Taking diracx if no extension:
    if DiracEntryPoint.CORE in entry_points_dict:
        return entry_points_dict[DiracEntryPoint.CORE].load()
