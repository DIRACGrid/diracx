from fastapi import Depends, FastAPI

from .db.auth.db import AuthDB
from .db.jobs.db import JobDB
from .routers import auth, job_manager

app = FastAPI(
    swagger_ui_init_oauth={
        "clientId": auth.DIRAC_CLIENT_ID,
        "scopes": "group:lhcb_user property:NormalUser",
        "usePkceWithAuthorizationCodeGrant": True,
    },
)

app.include_router(
    auth.router,
    prefix="/auth",
)
app.include_router(
    job_manager.router,
    prefix="/jobs",
    dependencies=[Depends(auth.verify_dirac_token)],
)


@app.on_event("startup")
async def startup():
    await JobDB.make_engine("sqlite+aiosqlite:///:memory:")
    await AuthDB.make_engine("sqlite+aiosqlite:///:memory:")


@app.on_event("shutdown")
async def shutdown():
    await JobDB.destroy_engine()
    await AuthDB.destroy_engine()


@app.get("/")
async def root():
    return {"message": "Hello Bigger Applications!"}


@app.get("/.well-known/openid-configuration")
async def openid_configuration():
    return {
        "issuer": auth.ISSUER,
        "token_endpoint": "http://localhost:8000/auth/lhcb/token",
        "authorization_endpoint": "http://localhost:8000/auth/lhcb/authorize",
        # "introspection_endpoint":"",
        # "userinfo_endpoint":"",
        "grant_types_supported": [
            "authorization_code",
            "urn:ietf:params:oauth:grant-type:device_code",
        ],
        "scopes_supported": [
            "group:lhcb_user",
            "property:NormalUser",
            "property:FileCatalogManagement",
        ],
        "response_types_supported": ["code"],
        "token_endpoint_auth_signing_alg_values_supported": [auth.ALGORITHM],
        "token_endpoint_auth_methods_supported": ["none"],
        "code_challenge_methods_supported": ["S256"],
        "device_authorization_endpoint": "http://localhost:8000/auth/lhcb/device",
    }
