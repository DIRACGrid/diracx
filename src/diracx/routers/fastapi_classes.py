from __future__ import annotations

import asyncio
import contextlib

from fastapi import APIRouter, FastAPI
from pydantic import BaseSettings

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


# router = APIRouter(
#     tags=["jobs"],
#     dependencies=[
#         has_properties(
#             SecurityProperty.NORMAL_USER | SecurityProperty.JOB_ADMINISTRATOR
#         )
#     ],
# )


class ServiceSettingsBase(BaseSettings, allow_mutation=False):
    @classmethod
    def create(cls):
        return cls()


class DiracRouter(APIRouter):
    def __init__(self, *, tags, dependencies=None, settings_class):
        super().__init__(tags=tags, dependencies=dependencies)
        self.settings_class = settings_class
