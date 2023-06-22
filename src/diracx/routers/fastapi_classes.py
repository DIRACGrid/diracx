from __future__ import annotations

import asyncio
import contextlib
import inspect
from functools import partial
from importlib.metadata import entry_points

from fastapi import APIRouter, FastAPI
from pydantic import BaseSettings

from diracx.db.utils import DiracDB

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
                "clientId": "myDIRACClientID",
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

    def include_router(self, router: APIRouter, *args, settings=None, **kwargs):
        super().include_router(router, *args, **kwargs)
        # If the router is a DiracRouter
        if not isinstance(router, DiracRouter):
            assert settings is None
            return

        assert settings is not None
        self.dependency_overrides[settings.create] = lambda: settings
        for db in settings.databases:
            assert db.__class__ not in self.dependency_overrides
            self.lifetime_functions.append(db.engine_context)
            self.dependency_overrides[db.__class__] = partial(lambda xxx: xxx, db)


class ServiceSettingsBase(BaseSettings, allow_mutation=False):
    @classmethod
    def create(cls):
        return cls()

    @property
    def databases(self):
        annotations = inspect.get_annotations(self.__class__, eval_str=True)
        for field, metadata in annotations.items():
            for annotation in getattr(metadata, "__metadata__", tuple()):
                if not isinstance(annotation, DiracDB):
                    continue
                for entry_point in entry_points().select(
                    group="diracx.dbs", name=annotation.name
                ):
                    yield entry_point.load()(getattr(self, field))
                    break
                else:
                    raise NotImplementedError(
                        f"Failed to find DB named {annotation.name}"
                    )


class DiracRouter(APIRouter):
    def __init__(
        self,
        *,
        dependencies=None,
        settings_class: type[ServiceSettingsBase],
        require_auth: bool = True,
    ):
        super().__init__(dependencies=dependencies)
        self.diracx_settings_class = settings_class
        self.diracx_require_auth = require_auth
