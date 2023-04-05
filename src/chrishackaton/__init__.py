from fastapi import FastAPI, Depends
from .routers import auth, job_manager

from .db.jobs.db import JobDB

app = FastAPI()

app.include_router(
    auth.router,
    prefix="/auth",
)
app.include_router(
    job_manager.router, prefix="/jobs", dependencies=[Depends(auth.verify_dirac_token)]
)

@app.on_event("startup")
async def startup():
    await JobDB.make_engine("sqlite+aiosqlite:///:memory:")


@app.on_event("shutdown")
async def shutdown():
    await JobDB.destroy_engine()


@app.get("/")
async def root():
    return {"message": "Hello Bigger Applications!"}
