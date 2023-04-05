from fastapi import FastAPI

from .routers import auth, job_manager
# from .db import jobs_db

app = FastAPI()

app.include_router(
    auth.router,
    prefix="/auth",
)
app.include_router(
    job_manager.router,
    prefix="/jobs",
)

# @app.on_event("startup")
# async def startup():
#     await jobs_db.connect()


# @app.on_event("shutdown")
# async def shutdown():
#     await jobs_db.disconnect()


@app.get("/")
async def root():
    return {"message": "Hello Bigger Applications!"}
